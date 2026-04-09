import fs from 'fs'
import path from 'path'
import { execFileSync } from 'child_process'
import * as core from '@actions/core'
import * as tc from '@actions/tool-cache'
import config from './config.js'

/**
 * R2 (S3-compatible) を使った disk_cache の sync。
 * Rust 製 bazel-r2-sync バイナリで manifest ベースの並列同期を行う。
 */

const R2_SYNC_VERSION = '0.1.0'
const TOOL_NAME = 'bazel-r2-sync'

async function getR2SyncBinary() {
  let toolPath = tc.find(TOOL_NAME, R2_SYNC_VERSION)
  if (toolPath) {
    core.debug(`Found bazel-r2-sync in cache @ ${toolPath}`)
    return path.join(toolPath, TOOL_NAME)
  }

  const url = `https://github.com/ippoan/setup-bazel/releases/download/r2-sync-v${R2_SYNC_VERSION}/${TOOL_NAME}`
  core.info(`Downloading bazel-r2-sync from ${url}`)
  const downloaded = await tc.downloadTool(url)
  fs.chmodSync(downloaded, '755')
  toolPath = await tc.cacheFile(downloaded, TOOL_NAME, TOOL_NAME, R2_SYNC_VERSION)
  return path.join(toolPath, TOOL_NAME)
}

function r2SyncArgs(subcommand, diskPath) {
  return [
    subcommand,
    '--endpoint', config.r2.endpoint,
    '--bucket', config.r2.bucket,
    '--prefix', 'disk-cache/',
    '--local-path', diskPath,
    '--concurrency', '64',
  ]
}

/**
 * R2 から disk_cache ディレクトリに同期 (restore)
 */
export async function restoreDiskCacheFromR2(diskPath) {
  if (!config.r2.enabled) return false

  try {
    const binary = await getR2SyncBinary()
    execFileSync(binary, r2SyncArgs('restore', diskPath), {
      env: {
        ...process.env,
        AWS_ACCESS_KEY_ID: config.r2.accessKeyId,
        AWS_SECRET_ACCESS_KEY: config.r2.secretAccessKey,
      },
      stdio: 'inherit',
      timeout: 300000, // 5 minutes
    })
    return true
  } catch (error) {
    core.warning(`Failed to restore disk cache from R2: ${error.message}`)
    return false
  }
}

/**
 * disk_cache ディレクトリを R2 に同期 (save)
 */
export async function saveDiskCacheToR2(diskPath) {
  if (!config.r2.enabled) return

  try {
    const binary = await getR2SyncBinary()
    execFileSync(binary, r2SyncArgs('save', diskPath), {
      env: {
        ...process.env,
        AWS_ACCESS_KEY_ID: config.r2.accessKeyId,
        AWS_SECRET_ACCESS_KEY: config.r2.secretAccessKey,
      },
      stdio: 'inherit',
      timeout: 600000, // 10 minutes
    })
  } catch (error) {
    core.warning(`Failed to save disk cache to R2: ${error.message}`)
  }
}
