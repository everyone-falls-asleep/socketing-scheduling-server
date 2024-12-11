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

async function getReservations(eventId, eventDateId) {
  const query = `
    SELECT
      seat.id AS seat_id,
      seat."areaId" AS area_id,
      reservation.id AS reservation_id
    FROM seat
    INNER JOIN area ON area.id = seat."areaId" AND area."deletedAt" IS NULL
    LEFT JOIN reservation ON reservation."seatId" = seat.id AND reservation."deletedAt" IS NULL
    WHERE area."eventId" = $1 AND (reservation."eventDateId" = $2 OR reservation."eventDateId" IS NULL);
  `;
  const params = [eventId, eventDateId];

  const { rows } = await fastify.pg.query(query, params);

  return rows;
}

fastify.get("/scheduling/liveness", (request, reply) => {
  reply.send({ status: "ok", message: "The server is alive." });
});

fastify.post("/scheduling/reservation/status", async (request, reply) => {
  try {
    await request.jwtVerify();
    const decodedToken = await request.jwtDecode();
    const { sub, eventId, eventDateId } = decodedToken;

    if (sub !== "scheduling-reservation-status") {
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

    if (!jobs.has(queueName)) {
      jobs.add(queueName);
      CronJob.from({
        cronTime: DateTime.now().plus({ seconds: 1 }).toJSDate(),
        onTick: async function () {
          console.log(`Task executed at: ${DateTime.now().toISO()}`);

          const seatsInfo = await getReservations(eventId, eventDateId);

          io.to(queueName).emit("seatsInfo", { seatsInfo });

          if ((await fastify.redis.llen(queueName)) === 0) {
            this.stop();
          } else {
            // 다음 실행 시간을 5초 후로 설정
            const nextExecution = DateTime.now().plus({ seconds: 5 });
            this.setTime(new CronTime(nextExecution.toJSDate()));
            this.start(); // 변경 후 작업 재시작 필요
          }
        },
        onComplete: function () {
          console.log(`Cron job has been stopped: ${queueName}`);
          jobs.delete(queueName);
        },
        start: true,
        timeZone: "Asia/Seoul",
      });

      return reply.status(201).send({
        status: "success",
        message: "Cron job created successfully.",
        data: { queueName },
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
