import fs from 'fs'
import { execSync } from 'child_process'
import { setTimeout } from 'timers/promises'
import * as core from '@actions/core'
import * as cache from '@actions/cache'
import * as github from '@actions/github'
import * as glob from '@actions/glob'
import * as tc from '@actions/tool-cache'
import config from './config.js'

async function run() {
  try {
    await setupBazel()
  } catch (error) {
    core.setFailed(error.stack)
  }
}

async function setupBazel() {
  core.startGroup('Configure Bazel')
  core.info('Configuration:')
  core.info(JSON.stringify(config, null, 2))

  await setupBazelrc()
  core.endGroup()

  await setupBazelisk()
  await restoreCache(config.bazeliskCache)
  await restoreDiskCache()
  await restoreCache(config.repositoryCache)
  await restoreExternalCaches(config.externalCache)
}

/**
 * disk-cache を tar.zst 1ファイルとして restore。
 * actions/cache が 1 ファイルだけ扱うので高速。
 * 展開は自前で tar --zstd -xf (5448 files でも <1s)。
 */
async function restoreDiskCache() {
  const diskCache = config.diskCache
  if (!diskCache.enabled) return

  core.startGroup('Restore disk cache (tar.zst)')
  try {
    const tarPath = config.diskCacheTarPath
    const hash = await glob.hashFiles(diskCache.files.join('\n'))
    const restoreKey = `${config.baseCacheKey}-disk-tar-`
    const key = `${restoreKey}${hash}`

    let t0 = Date.now()
    const restoredKey = await cache.restoreCache(
      [tarPath], key, [restoreKey],
      { segmentTimeoutInMs: 300000 }
    )
    const downloadMs = Date.now() - t0

    if (restoredKey) {
      core.info(`Cache downloaded in ${(downloadMs / 1000).toFixed(1)}s (from ${restoredKey})`)
      if (fs.existsSync(tarPath)) {
        const stat = fs.statSync(tarPath)
        core.info(`tar.zst size: ${(stat.size / 1024 / 1024).toFixed(1)} MB`)
        const diskDir = diskCache.paths[0]
        fs.mkdirSync(diskDir, { recursive: true })
        t0 = Date.now()
        execSync(`tar --zstd -xf ${tarPath} -C /`, { stdio: 'inherit' })
        const extractMs = Date.now() - t0
        core.info(`Extracted in ${(extractMs / 1000).toFixed(1)}s`)
      }
      if (restoredKey === key) {
        core.saveState('disk-tar-cache-hit', 'true')
      }
    } else {
      core.info('No disk cache found')
    }
  } catch (err) {
    core.warning(`Failed to restore disk cache: ${err}`)
  } finally {
    core.endGroup()
  }
}

async function setupBazelisk() {
  if (config.bazeliskVersion.length == 0) {
    return
  }

  core.startGroup('Setup Bazelisk')
  let toolPath = tc.find('bazelisk', config.bazeliskVersion)
  if (toolPath) {
    core.debug(`Found in cache @ ${toolPath}`)
  } else {
    toolPath = await downloadBazelisk()
  }
  core.addPath(toolPath)
  core.endGroup()
}

async function downloadBazelisk() {
  const version = config.bazeliskVersion
  core.debug(`Attempting to download ${version}`)

  // Possible values are 'arm', 'arm64', 'ia32', 'mips', 'mipsel', 'ppc', 'ppc64', 's390', 's390x' and 'x64'.
  // Bazelisk filenames use 'amd64' and 'arm64'.
  let arch = config.os.arch
  if (arch == 'x64') {
    arch = 'amd64'
  }

  // Possible values are 'aix', 'darwin', 'freebsd', 'linux', 'openbsd', 'sunos' and 'win32'.
  // Bazelisk filenames use 'darwin', 'linux' and 'windows'.
  let platform = config.os.platform
  if (platform == "win32") {
    platform = "windows"
  }

  let filename = `bazelisk-${platform}-${arch}`
  if (platform == 'windows') {
    filename = `${filename}.exe`
  }

  const token = process.env.BAZELISK_GITHUB_TOKEN
  const octokit = github.getOctokit(token, {
    baseUrl: 'https://api.github.com'
  })
  const { data: releases } = await octokit.rest.repos.listReleases({
    owner: 'bazelbuild',
    repo: 'bazelisk'
  })

  // Find version matching semver specification.
  const tagName = tc.evaluateVersions(releases.map((r) => r.tag_name), version)
  const release = releases.find((r) => r.tag_name === tagName)
  if (!release) {
    throw new Error(`Unable to find Bazelisk version ${version}`)
  }

  const asset = release.assets.find((a) => a.name == filename)
  if (!asset) {
    throw new Error(`Unable to find Bazelisk version ${version} for platform ${platform}/${arch}`)
  }

  const url = asset.browser_download_url
  core.debug(`Downloading from ${url}`)
  const downloadPath = await tc.downloadTool(url, undefined, `token ${token}`)

  core.debug('Adding to the cache...');
  fs.chmodSync(downloadPath, '755');
  let bazel_name = "bazel";
  if (platform == 'windows') {
    bazel_name = `${bazel_name}.exe`
  }
  const cachePath = await tc.cacheFile(downloadPath, bazel_name, 'bazelisk', version)
  core.debug(`Successfully cached bazelisk to ${cachePath}`)

  return cachePath
}

async function setupBazelrc() {
  for (const bazelrcPath of config.paths.bazelrc) {
    fs.writeFileSync(
      bazelrcPath,
      `startup --output_base=${config.paths.bazelOutputBase}\n`
    )
    fs.appendFileSync(bazelrcPath, config.bazelrc.join("\n"))
  }
}

async function restoreExternalCaches(cacheConfig) {
  if (!cacheConfig.enabled) {
    return
  }

  // First fetch the manifest of external caches used.
  const path = cacheConfig.manifest.path
  await restoreCache({
    enabled: true,
    files: cacheConfig.manifest.files,
    name: cacheConfig.manifest.name,
    paths: [path]
  })

  // Now restore all external caches defined in manifest
  if (fs.existsSync(path)) {
    const manifest = fs.readFileSync(path, { encoding: 'utf8' })
    const restorePromises = manifest.split('\n').filter(s => s)
      .map(name => {
        return restoreCache({
          enabled: cacheConfig[name]?.enabled ?? cacheConfig.default.enabled,
          files: cacheConfig[name]?.files || cacheConfig.default.files,
          name: cacheConfig.default.name(name),
          paths: cacheConfig.default.paths(name)
        });
      });
    await Promise.all(restorePromises);
  }
}

async function restoreCache(cacheConfig) {
  if (!cacheConfig.enabled) {
    return
  }

  const delay = Math.random() * 1000 // timeout <= 1 sec to reduce 429 errors
  await setTimeout(delay)

  core.startGroup(`Restore cache for ${cacheConfig.name}`)
  const name = cacheConfig.name
  try {
    const hash = await glob.hashFiles(cacheConfig.files.join('\n'))
    const paths = cacheConfig.paths
    const restoreKey = `${config.baseCacheKey}-${name}-`
    const key = `${restoreKey}${hash}`

    core.debug(`Attempting to restore ${name} cache from ${key}`)

    const restoredKey = await cache.restoreCache(
      paths, key, [restoreKey],
      { segmentTimeoutInMs: 300000 } // 5 minutes
    )

    if (restoredKey) {
      core.info(`Successfully restored cache from ${restoredKey}`)

      if (restoredKey === key) {
        core.saveState(`${name}-cache-hit`, 'true')
      }
    } else {
      core.info(`Failed to restore ${name} cache`)
    }
  } catch (err) {
    core.warning(`Failed to restore ${name} cache with error: ${err}`)
  } finally {
    core.endGroup()
  }
}

run()
