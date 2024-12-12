import Fastify from "fastify";
import fastifyEnv from "@fastify/env";
import cors from "@fastify/cors";
import fastifyJwt from "@fastify/jwt";
import fastifyRedis from "@fastify/redis";
import fastifyPostgres from "@fastify/postgres";
import { Server } from "socket.io";
import { createAdapter } from "@socket.io/redis-adapter";
import { CronJob, CronTime } from "cron";
import { DateTime } from "luxon";
import crypto from "node:crypto";

// field          allowed values
// -----          --------------
// second         0-59
// minute         0-59
// hour           0-23
// day of month   1-31
// month          1-12 (or names, see below)
// day of week    0-7 (0 or 7 is Sunday, or use names)

const schema = {
  type: "object",
  required: ["PORT", "JWT_SECRET", "CACHE_HOST", "CACHE_PORT", "DB_URL"],
  properties: {
    PORT: {
      type: "integer",
    },
    JWT_SECRET: {
      type: "string",
    },
    CACHE_HOST: {
      type: "string",
    },
    CACHE_PORT: {
      type: "integer",
    },
    DB_URL: {
      type: "string",
    },
  },
};

const jobs = new Set();

const fastify = Fastify({
  trustProxy: true,
  logger: true,
});

await fastify.register(fastifyEnv, {
  schema,
  dotenv: true,
});

await fastify.register(cors, {
  origin: "*",
});

await fastify.register(fastifyJwt, {
  secret: fastify.config.JWT_SECRET,
});

await fastify.register(fastifyRedis, {
  host: fastify.config.CACHE_HOST,
  port: fastify.config.CACHE_PORT,
  family: 4,
});

await fastify.register(fastifyPostgres, {
  connectionString: fastify.config.DB_URL,
});

// fastify.addHook("onRequest", async (request, reply) => {
//   try {
//     await request.jwtVerify();
//     console.log(await request.jwtDecode());
//   } catch (err) {
//     reply.send(err);
//   }
// });

function decorrelatedJitter(baseDelay, maxDelay, previousDelay) {
  if (!previousDelay) {
    previousDelay = baseDelay;
  }
  return Math.min(
    maxDelay,
    Math.random() * (previousDelay * 3 - baseDelay) + baseDelay
  );
}

async function getReservations(eventId, eventDateId) {
  const query = `
    SELECT
      seat.id AS seat_id
    FROM seat
    INNER JOIN area ON area.id = seat."areaId" AND area."deletedAt" IS NULL
    LEFT JOIN reservation ON reservation."seatId" = seat.id AND reservation."deletedAt" IS NULL
    WHERE area."eventId" = $1 AND reservation."eventDateId" = $2;
  `;
  const params = [eventId, eventDateId];

  const { rows } = await fastify.pg.query(query, params);

  return rows;
}

async function getAllAreasFromRedis(roomName) {
  const areasData = await fastify.redis.hgetall(`areas:${roomName}`);
  const areas = [];
  for (const areaId in areasData) {
    areas.push(JSON.parse(areasData[areaId]));
  }
  return areas;
}

async function getAreasForRoom(eventId) {
  // PostgreSQL 쿼리 실행
  const query = `
    SELECT
      area.id,
      area.label,
      area.svg,
      area.price
    FROM area
    WHERE area."eventId" = $1
      AND area."deletedAt" IS NULL;
  `;
  const params = [eventId];

  const { rows } = await fastify.pg.query(query, params);

  // 구역 정보를 반환
  return rows;
}

async function updateAreaInRedis(roomName, areaId, area) {
  await fastify.redis.hset(`areas:${roomName}`, areaId, JSON.stringify(area));
}

async function getAllSeatsFromRedis(areaName) {
  const seatsData = await fastify.redis.hgetall(`seats:${areaName}`);
  const seats = [];
  for (const seatId in seatsData) {
    seats.push(JSON.parse(seatsData[seatId]));
  }
  return seats;
}

async function getSeatsForArea(eventDateId, areaId) {
  // PostgreSQL 쿼리 실행
  const query = `
    SELECT
      seat.id AS seat_id,
      seat.cx,
      seat.cy,
      seat.row,
      seat.number,
      seat."areaId" AS area_id,
      reservation.id AS reservation_id,
      eventDate.id AS event_date_id,
      eventDate.date,
      "order"."userId" AS reserved_user_id
    FROM seat
    LEFT JOIN reservation ON reservation."seatId" = seat.id AND reservation."canceledAt" IS NULL AND reservation."deletedAt" IS NULL
    LEFT JOIN event_date AS eventDate ON reservation."eventDateId" = eventDate.id
    LEFT JOIN "order" ON reservation."orderId" = "order".id AND "order"."canceledAt" IS NULL AND "order"."deletedAt" IS NULL
    WHERE seat."areaId" = $1
      AND (eventDate.id = $2 OR eventDate.id IS NULL);
  `;
  const params = [areaId, eventDateId];

  const { rows } = await fastify.pg.query(query, params);

  // 데이터 가공
  const seatMap = new Map();

  rows.forEach((row) => {
    if (!seatMap.has(row.seat_id)) {
      seatMap.set(row.seat_id, {
        id: row.seat_id,
        cx: row.cx,
        cy: row.cy,
        row: row.row,
        number: row.number,
        area_id: row.area_id,
        selectedBy: null,
        reservedUserId: row.reserved_user_id || null, // 예약된 유저 ID
        updatedAt: null, // 초기 상태
        expirationTime: null, // 초기 상태
      });
    }
  });

  return Array.from(seatMap.values());
}

async function updateSeatInRedis(areaName, seatId, seat) {
  await fastify.redis.hset(`seats:${areaName}`, seatId, JSON.stringify(seat));
}

async function startReservationStatusInterval(eventId, eventDateId) {
  const roomName = `${eventId}_${eventDateId}`;
  // 만약 해당 room에 대한 타이머가 없다면 생성
  try {
    // areas 및 areaStats 계산 로직
    let areas = await getAllAreasFromRedis(roomName);
    if (areas.length === 0) {
      areas = await getAreasForRoom(eventId);
      for (const area of areas) {
        await updateAreaInRedis(roomName, area.id, area);
      }
    }

    const areaStats = [];
    for (const area of areas) {
      const areaId = area.id;
      const areaName = `${eventId}_${eventDateId}_${areaId}`;
      let seats = await getAllSeatsFromRedis(areaName);
      if (seats.length === 0) {
        seats = await getSeatsForArea(eventDateId, areaId);
        for (const seat of seats) {
          await updateSeatInRedis(areaName, seat.id, seat);
        }
      }

      const totalSeatsNum = seats.length;
      const reservedSeatsNum = seats.filter(
        (seat) => seat.reservedUserId !== null
      ).length;
      areaStats.push({
        areaId: areaId,
        totalSeatsNum: totalSeatsNum,
        reservedSeatsNum: reservedSeatsNum,
      });
    }

    // 해당 room에 통계 정보 전송
    io.to(roomName).emit("reservedSeatsStatistic", areaStats);
  } catch (error) {
    fastify.log.error(
      `Error emitting reservedSeatsStatistic: ${error.message}`
    );
  }
}

async function getRoomUserCount(roomName) {
  const maxRetries = 30;
  let delay = null;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      // const sockets = await io.in(roomName).fetchSockets(); // 모든 노드에서 룸에 속한 소켓 ID 가져오기
      const count = await fastify.redis.get(`room:${roomName}:count`);
      return parseInt(count || "0"); // 소켓 수 반환
    } catch (err) {
      console.error(
        `Timeout reached, retrying (attempt ${attempt}/${maxRetries})...`
      );
      await new Promise((resolve) => {
        delay = decorrelatedJitter(100, 60000, delay);
        setTimeout(resolve, delay);
      });
    }
  }
}

async function getQueueLength(queueName) {
  try {
    return await fastify.redis.llen(queueName);
  } catch (err) {
    console.error("Redis error:", err);
    return -1;
  }
}

async function getQueue(queueName) {
  const rawQueue = await fastify.redis.lrange(queueName, 0, -1);
  return rawQueue
    .map((item) => {
      try {
        return JSON.parse(item);
      } catch (err) {
        console.error(`Failed to parse item in queue "${queueName}":`, err);
        return null; // 파싱 실패 시 null로 반환 (필요에 따라 처리 방식 변경 가능)
      }
    })
    .filter((item) => item !== null); // null 값 제거
}

async function getSocketsInRoom(queueName) {
  const maxRetries = 30;
  let delay = null;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await io.in(queueName).fetchSockets();
    } catch (err) {
      console.error(
        `Timeout reached, retrying (attempt ${attempt}/${maxRetries})...`
      );
      await new Promise((resolve) => {
        delay = decorrelatedJitter(100, 60000, delay);
        setTimeout(resolve, delay);
      });
    }
  }
}

async function broadcastQueueUpdate(queueName) {
  const queue = await getQueue(queueName);
  const socketsInRoom = await getSocketsInRoom(queueName);
  socketsInRoom.forEach((socket) => {
    const position = queue.findIndex((item) => item.socketId === socket.id) + 1;
    socket.emit("updateQueue", {
      yourPosition: position,
      totalWaiting: queue.length,
    });
  });
}

fastify.get("/scheduling/liveness", (request, reply) => {
  reply.send({ status: "ok", message: "The server is alive." });
});

// fastify.post("/scheduling/token", async (request, reply) => {
//   const { eventId, eventDateId } = request.body;

//   if (!eventId || !eventDateId) {
//     return reply.status(400).send({
//       status: "error",
//       message: "Missing required token payload: eventId or eventDateId.",
//     });
//   }

//   try {
//     const token = fastify.jwt.sign(
//       {
//         jti: crypto.randomUUID(),
//         sub: "scheduling-reservation-status",
//         eventId,
//         eventDateId,
//       },
//       {
//         expiresIn: 600,
//       }
//     );

//     return reply.status(403).send({
//       token,
//     });
//   } catch (err) {
//     console.error(err);
//     return reply.status(500).send({
//       status: "error",
//       message: "Internal Server Error",
//       error: err.message,
//     });
//   }
// });

fastify.post("/scheduling/reservation/status", async (request, reply) => {
  try {
    await request.jwtVerify();
    const decodedToken = await request.jwtDecode();
    const { sub, eventId, eventDateId } = decodedToken;

    if (sub !== "scheduling") {
      return reply.status(403).send({
        status: "error",
        message: "Invalid token subject. Unauthorized request.",
      });
    }

    if (!eventId || !eventDateId) {
      return reply.status(400).send({
        status: "error",
        message: "Missing required token payload: eventId or eventDateId.",
      });
    }

    const queueName = `queue:${eventId}_${eventDateId}`;
    const jobName = `reservation:status:${queueName}`;

    if (!jobs.has(jobName)) {
      jobs.add(jobName);

      let isStopped = false;

      CronJob.from({
        cronTime: DateTime.now().plus({ seconds: 5 }).toJSDate(),
        onTick: async function () {
          try {
            const queueSize = await getQueueLength(queueName);

            if (queueSize === 0) {
              console.log(
                `Queue is empty. Stopping the cron job for queue: ${queueName}`
              );
              isStopped = true; // 플래그 설정
              this.stop();
            } else {
              const seatsInfo = await getReservations(eventId, eventDateId);
              io.to(queueName).emit("seatsInfo", { seatsInfo });

              // 다음 실행 시간을 5초 후로 설정
              const nextExecution = DateTime.now().plus({ seconds: 5 });
              this.setTime(new CronTime(nextExecution.toJSDate()));
              this.start(); // 변경 후 작업 재시작 필요

              console.log(
                `Task(reservation:status) executed at: ${DateTime.now().toISO()}`
              );
            }
          } catch (err) {
            console.error(err);
            isStopped = true; // 에러 발생 시에도 작업 중지
            this.stop();
          }
        },
        onComplete: function () {
          if (isStopped) {
            console.log(`Cron job has been stopped: ${jobName}`);
            jobs.delete(jobName);
          }
        },
        start: true,
        runOnInit: true,
        timeZone: "Asia/Seoul",
      });

      return reply.status(201).send({
        status: "success",
        message: "Cron job created successfully.",
        data: { jobName },
      });
    } else {
      return reply.status(409).send({
        status: "fail",
        message: "A cron job is already running for the provided event.",
      });
    }
  } catch (err) {
    console.error(err);
    return reply.status(500).send({
      status: "error",
      message: "Internal Server Error",
      error: err.message,
    });
  }
});

fastify.post("/scheduling/queue/status", async (request, reply) => {
  try {
    await request.jwtVerify();
    const decodedToken = await request.jwtDecode();
    const { sub, eventId, eventDateId } = decodedToken;

    if (sub !== "scheduling") {
      return reply.status(403).send({
        status: "error",
        message: "Invalid token subject. Unauthorized request.",
      });
    }

    if (!eventId || !eventDateId) {
      return reply.status(400).send({
        status: "error",
        message: "Missing required token payload: eventId or eventDateId.",
      });
    }

    const queueName = `queue:${eventId}_${eventDateId}`;
    const jobName = `queue:status:${queueName}`;

    if (!jobs.has(jobName)) {
      jobs.add(jobName);

      let isStopped = false;

      CronJob.from({
        cronTime: DateTime.now().plus({ seconds: 1 }).toJSDate(),
        onTick: async function () {
          try {
            const queueSize = await getQueueLength(queueName);

            if (queueSize === 0) {
              console.log(
                `Queue is empty. Stopping the cron job for queue: ${queueName}`
              );
              isStopped = true; // 플래그 설정
              this.stop();
            } else {
              await broadcastQueueUpdate(queueName);

              // 다음 실행 시간을 1초 후로 설정
              const nextExecution = DateTime.now().plus({ seconds: 1 });
              this.setTime(new CronTime(nextExecution.toJSDate()));
              this.start(); // 변경 후 작업 재시작 필요

              console.log(
                `Task(queue:status) executed at: ${DateTime.now().toISO()}`
              );
            }
          } catch (err) {
            console.error(err);
            isStopped = true; // 에러 발생 시에도 작업 중지
            this.stop();
          }
        },
        onComplete: function () {
          if (isStopped) {
            console.log(`Cron job has been stopped: ${jobName}`);
            jobs.delete(jobName);
          }
        },
        start: true,
        runOnInit: true,
        timeZone: "Asia/Seoul",
      });

      return reply.status(201).send({
        status: "success",
        message: "Cron job created successfully.",
        data: { jobName },
      });
    } else {
      return reply.status(409).send({
        status: "fail",
        message: "A cron job is already running for the provided event.",
      });
    }
  } catch (err) {
    console.error(err);
    return reply.status(500).send({
      status: "error",
      message: "Internal Server Error",
      error: err.message,
    });
  }
});

fastify.post(
  "/scheduling/seat/reservation/statistic",
  async (request, reply) => {
    try {
      await request.jwtVerify();
      const decodedToken = await request.jwtDecode();
      const { sub, eventId, eventDateId } = decodedToken;

      if (sub !== "scheduling") {
        return reply.status(403).send({
          status: "error",
          message: "Invalid token subject. Unauthorized request.",
        });
      }

      if (!eventId || !eventDateId) {
        return reply.status(400).send({
          status: "error",
          message: "Missing required token payload: eventId or eventDateId.",
        });
      }

      const roomName = `${eventId}_${eventDateId}`;
      const jobName = `seat:reservation:statistic:${roomName}`;

      if (!jobs.has(jobName)) {
        jobs.add(jobName);

        let isStopped = false;

        CronJob.from({
          cronTime: DateTime.now().plus({ seconds: 2 }).toJSDate(),
          onTick: async function () {
            try {
              const roomSize = await getRoomUserCount(roomName);

              if (roomSize === 0) {
                console.log(
                  `Room is empty. Stopping the cron job for room: ${roomName}`
                );
                isStopped = true; // 플래그 설정
                this.stop();
              } else {
                await startReservationStatusInterval(eventId, eventDateId);

                // 다음 실행 시간을 2초 후로 설정
                const nextExecution = DateTime.now().plus({ seconds: 2 });
                this.setTime(new CronTime(nextExecution.toJSDate()));
                this.start(); // 변경 후 작업 재시작 필요

                console.log(
                  `Task(seat:reservation:statistic) executed at: ${DateTime.now().toISO()}`
                );
              }
            } catch (err) {
              console.error(err);
              isStopped = true; // 에러 발생 시에도 작업 중지
              this.stop();
            }
          },
          onComplete: function () {
            if (isStopped) {
              console.log(`Cron job has been stopped: ${jobName}`);
              jobs.delete(jobName);
            }
          },
          start: true,
          runOnInit: true,
          timeZone: "Asia/Seoul",
        });

        return reply.status(201).send({
          status: "success",
          message: "Cron job created successfully.",
          data: { jobName },
        });
      } else {
        return reply.status(409).send({
          status: "fail",
          message: "A cron job is already running for the provided event.",
        });
      }
    } catch (err) {
      console.error(err);
      return reply.status(500).send({
        status: "error",
        message: "Internal Server Error",
        error: err.message,
      });
    }
  }
);

const pubClient = fastify.redis.duplicate();
const subClient = fastify.redis.duplicate();

const io = new Server(fastify.server, {
  cors: {
    origin: "*",
    methods: "*",
    credentials: true,
  },
  transports: ["websocket"],
  adapter: createAdapter(pubClient, subClient),
});

// 실시간 서버 시간 브로드캐스트
setInterval(() => {
  const serverTime = new Date().toISOString();
  io.emit("serverTime", serverTime);
}, 1000); // 1초마다 서버 시간 전송

const startServer = async () => {
  try {
    const address = await fastify.listen({
      port: fastify.config.PORT,
      host: "0.0.0.0",
    });

    fastify.log.info(`Server is now listening on ${address}`);

    if (process.send) {
      process.send("ready");
    }
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

let shutdownInProgress = false; // 중복 호출 방지 플래그

async function gracefulShutdown(signal) {
  if (shutdownInProgress) {
    fastify.log.warn(
      `Shutdown already in progress. Ignoring signal: ${signal}`
    );
    return;
  }
  shutdownInProgress = true; // 중복 호출 방지

  fastify.log.info(`Received signal: ${signal}. Starting graceful shutdown...`);

  try {
    await fastify.close();
    fastify.log.info("Fastify server has been closed.");

    // 기타 필요한 종료 작업 (예: DB 연결 해제)
    // await database.disconnect();
    fastify.log.info("Additional cleanup tasks completed.");

    fastify.log.info("Graceful shutdown complete. Exiting process...");
    process.exit(0);
  } catch (error) {
    fastify.log.error("Error occurred during graceful shutdown:", error);
    process.exit(1);
  }
}

startServer();

process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));

// const job = CronJob.from({
//   cronTime: "*/5 * * * * *",
//   onTick: (() => {
//     // let executionsLeft = 3;
//     return function () {
//       console.log("You will see this message every second");
//       // executionsLeft--;
//       // if (executionsLeft <= 0) {
//       //   console.log("Stopping the Cron job after 3 executions.");
//       //   this.stop();
//       // }
//     };
//   })(),
//   onComplete: function () {
//     console.log("Cron job has been stopped");
//   },
//   // context: {},
//   start: false,
//   timeZone: "Asia/Seoul",
// });

// const job = CronJob.from({
//   cronTime: DateTime.now().plus({ seconds: 5 }).toJSDate(),
//   onTick: (() => {
//     let executionsLeft = 3;
//     return function () {
//       console.log(`Task executed at: ${DateTime.now().toISO()}`);

//       // 다음 실행 시간을 5초 후로 설정
//       const nextExecution = DateTime.now().plus({ seconds: 5 });

//       this.setTime(new CronTime(nextExecution.toJSDate()));
//       this.start(); // 변경 후 작업 재시작 필요

//       if (--executionsLeft <= 0) {
//         console.log("Stopping the Cron job after 3 executions.");
//         this.stop();
//       }
//     };
//   })(),
//   onComplete: function () {
//     console.log("Cron job has been stopped");
//   },
//   start: false,
//   timeZone: "Asia/Seoul",
// });

// job.start();

// setTimeout(() => {
//   job.stop();
// }, 3000);

// console.log(job.nextDates(5));
