import fs from 'fs'
import path from 'path'
import { execSync } from 'child_process'
import * as cache from '@actions/cache'
import * as core from '@actions/core'
import * as glob from '@actions/glob'
import config from './config.js'
import { getFolderSize } from './util.js'

async function run() {
  await saveCaches()
  process.exit(0)
}

async function saveCaches() {
  if (!config.cacheSave) {
    core.info('Cache saving is disabled (cache-save: false)')
    return
  }

  await saveCache(config.bazeliskCache)
  await saveDiskCache()
  await saveCache(config.repositoryCache)
  await saveExternalCaches(config.externalCache)
}

/**
 * disk-cache を tar.zst 1ファイルに圧縮して save。
 * actions/cache が 1 ファイルだけ扱うので高速。
 */
async function saveDiskCache() {
  const diskCache = config.diskCache
  if (!diskCache.enabled) return

  const cacheHit = core.getState('disk-tar-cache-hit')
  if (cacheHit === 'true') {
    core.info('Disk cache hit, skipping save')
    return
  }

  const diskDir = diskCache.paths[0]
  if (!fs.existsSync(diskDir)) {
    core.info('No disk cache directory, skipping save')
    return
  }

  core.startGroup('Save disk cache (tar.zst)')
  try {
    const tarPath = config.diskCacheTarPath

    // tar.zst に圧縮
    let t0 = Date.now()
    execSync(`tar --zstd -cf ${tarPath} ${diskDir}`, { stdio: 'inherit' })
    const compressMs = Date.now() - t0
    const stat = fs.statSync(tarPath)
    core.info(`Compressed in ${(compressMs / 1000).toFixed(1)}s (${(stat.size / 1024 / 1024).toFixed(1)} MB)`)

    // actions/cache に save
    const hash = await glob.hashFiles(
      diskCache.files.join('\n'),
      undefined,
      { followSymbolicLinks: false }
    )
    const key = `${config.baseCacheKey}-${diskCache.name}-tar-${hash}`
    t0 = Date.now()
    await cache.saveCache([tarPath], key)
    const uploadMs = Date.now() - t0
    core.info(`Uploaded in ${(uploadMs / 1000).toFixed(1)}s`)
  } catch (error) {
    core.warning(`Failed to save disk cache: ${error.stack}`)
  } finally {
    core.endGroup()
  }
}

async function saveExternalCaches(cacheConfig) {
  if (!cacheConfig.enabled) {
    return
  }

  const globber = await glob.create(
    `${config.paths.bazelExternal}/*`,
    { implicitDescendants: false }
  )
  const externalPaths = await globber.glob()
  const savedCaches = []

  for (const externalPath of externalPaths) {
    const size = await getFolderSize(externalPath)
    const sizeMB = (size / 1024 / 1024).toFixed(2)
    core.debug(`${externalPath} size is ${sizeMB}MB`)

    if (sizeMB >= cacheConfig.minSize) {
      const name = path.basename(externalPath)
      await saveCache({
        enabled: cacheConfig[name]?.enabled ?? cacheConfig.default.enabled,
        files: cacheConfig[name]?.files || cacheConfig.default.files,
        name: cacheConfig.default.name(name),
        paths: cacheConfig.default.paths(name)
      })
      savedCaches.push(name)
    }
  }

  if (savedCaches.length > 0) {
    const path = cacheConfig.manifest.path
    fs.writeFileSync(path, savedCaches.join('\n'))
    await saveCache({
      enabled: true,
      files: cacheConfig.manifest.files,
      name: cacheConfig.manifest.name,
      paths: [path]
    })
  }
}

async function saveCache(cacheConfig) {
  if (!cacheConfig.enabled) {
    return
  }

  const cacheHit = core.getState(`${cacheConfig.name}-cache-hit`)
  core.debug(`${cacheConfig.name}-cache-hit is ${cacheHit}`)
  if (cacheHit === 'true') {
    return
  }

  try {
    core.startGroup(`Save cache for ${cacheConfig.name}`)
    const paths = cacheConfig.paths
    const hash = await glob.hashFiles(
      cacheConfig.files.join('\n'),
      undefined,
      // We don't want to follow symlinks as it's extremely slow on macOS.
      { followSymbolicLinks: false }
    )
    const key = `${config.baseCacheKey}-${cacheConfig.name}-${hash}`
    core.debug(`Attempting to save ${paths} cache to ${key}`)
    await cache.saveCache(paths, key)
    core.info('Successfully saved cache')
  } catch (error) {
    core.warning(error.stack)
  } finally {
    core.endGroup()
  }
}

run()
