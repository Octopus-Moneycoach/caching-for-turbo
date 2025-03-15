import { Readable } from 'node:stream'
import { env } from '../env'
import { pipeline } from 'node:stream/promises'
import {
  createReadStream,
  createWriteStream,
  existsSync,
  statSync
} from 'node:fs'
import { getCacheClient } from './utils'
import {
  cacheVersion,
  getCacheKey,
  getFsCachePath,
  getTempCachePath
} from '../constants'
import cache from '@actions/cache'
import streamToPromise from 'stream-to-promise'

type RequestContext = {
  log: {
    info: (message: string) => void
  }
}

//* Cache API
export async function saveCache(
  ctx: RequestContext,
  hash: string,
  tag: string,
  stream: Readable
): Promise<void> {
  const tempFile = getFsCachePath(hash)
  const writeStream = createWriteStream(tempFile)
  await streamToPromise(stream.pipe(writeStream))
  const id = await cache.saveCache([tempFile], getCacheKey(hash, tag))
  ctx.log.info(`Saved cache ${id} for ${hash}`)
}

export async function getCache(
  ctx: RequestContext,
  hash: string
): Promise<
  [number | undefined, Readable | ReadableStream, string | undefined] | null
> {
  //* Get cache from filesystem if cache API env vars are not set
  if (!env.valid) {
    const path = getFsCachePath(hash)
    if (!existsSync(path)) return null
    const size = statSync(path).size
    return [size, createReadStream(path), undefined]
  }

  const path = getFsCachePath(hash)
  const key = await cache.restoreCache([path], getCacheKey(hash))
  if (!key) {
    return null
  }
  const [found, tag] = key.split('#')
  if (found !== key) {
    ctx.log.info(`Cache key mismatch: ${found} !== ${key}`)
    return null
  }

  const stream = createReadStream(path)
  return [stream.readableLength, stream, tag]
}
