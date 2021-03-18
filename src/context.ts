import { PrismaClient } from "@prisma/client"
import { AccountMonitor } from "./services/accountMonitor"
import { MessageClient } from "./services/messenger"
import { initSettings } from "./settings"
import { AsyncOpCleanupClient } from "./services/asyncOpCleanup";

initSettings()

export const prisma: PrismaClient = new PrismaClient()
export const messenger = new MessageClient(prisma)
export const accountMonitor = new AccountMonitor(prisma, messenger)
export const asyncOpCleanUp = new AsyncOpCleanupClient(prisma)
