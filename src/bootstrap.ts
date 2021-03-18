import { accountMonitor, asyncOpCleanUp } from "./context";
import { logger } from "./logger";

export function bootstrap() {
  accountMonitor.start()
  logger.info({ message: "Started account monitor" })

  asyncOpCleanUp.start()
  logger.info({ message: "Started asyncOpCleanup" })
}
