import { PrismaClient } from '@prisma/client';
import { resolveDatabaseUrl } from './database-url.js';

/** Avoid empty root `dev.db` when `prisma/dev.db` holds the real catalog (see database-url.ts). */
process.env.DATABASE_URL = resolveDatabaseUrl(process.env.DATABASE_URL);

const globalForPrisma = global as unknown as { prisma: PrismaClient };

export const prisma =
  globalForPrisma.prisma ||
  new PrismaClient({
    log: ['query'],
  });

if (process.env.NODE_ENV !== 'production') globalForPrisma.prisma = prisma;
