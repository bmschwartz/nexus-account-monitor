import AWS from "aws-sdk"
import winston from "winston";
import WinstonCloudWatch from "winston-cloudwatch";
import * as dotenv from "dotenv"

dotenv.config()

AWS.config.update({
  region: process.env.AWS_REGION as string,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY as string,
    secretAccessKey: process.env.AWS_SECRET_KEY as string,
  },
})

class Logger {
  _logger: winston.Logger

  constructor() {
    this._logger = this.initLogger()

    this._addTransports()

  }

  initLogger(): winston.Logger {
    return winston.createLogger({
      level: "info",
      format: winston.format.json(),
      defaultMeta: { service: "nexus-account-monitor" },
      transports: [
        //
        // - Write all logs with level `error` and below to `error.log`
        // - Write all logs with level `info` and below to `combined.log`
        //
        new winston.transports.File({ filename: "error.log", level: "error" }),
        new winston.transports.File({ filename: "combined.log" }),
      ],
    });
  }

  _addTransports() {
    this._logger.add(new WinstonCloudWatch({
      logGroupName: "nexus-account-monitor",
      logStreamName: process.env.NODE_ENV,
    }));

    //
    // If we're not in production then log to the `console` with the format:
    // `${info.level}: ${info.message} JSON.stringify({ ...rest }) `
    //
    if (process.env.NODE_ENV !== "production") {
      this._logger.add(new winston.transports.Console({
        format: winston.format.simple(),
      }));
    }
  }

  debug(data: object) {
    this._logger.debug(JSON.stringify(data))
  }

  info(data: object) {
    this._logger.info(JSON.stringify(data))
  }

  warn(data: object) {
    this._logger.warn(JSON.stringify(data))
  }

  error(data: object) {
    this._logger.error(JSON.stringify(data))
  }

}

export const logger = new Logger()
