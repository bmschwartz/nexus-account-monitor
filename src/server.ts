import express from "express"
import * as dotenv from "dotenv"
import { bootstrap } from "./bootstrap"
import { logger } from "./logger"

dotenv.config()

const app = express()
const port = process.env.PORT

app.get("/health", (req, res) => {
  return res.sendStatus(200)
})

app.listen(port, () => {
  logger.info({ message: `🚀 Server ready on ${port}` })
  bootstrap()
})
