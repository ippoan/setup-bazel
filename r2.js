import fs from 'fs'
import path from 'path'
import { spawn } from 'child_process'
import { execFileSync } from 'child_process'
import * as core from '@actions/core'
import * as tc from '@actions/tool-cache'
import config from './config.js'

/**
 * R2 (S3-compatible) を使った disk_cache の sync。
 * Rust 製 bazel-r2-sync バイナリで manifest ベースの並列同期を行う。
 * watch モード: build 中にバックグラウンドで新ファイルを即座にアップロード。
 */

const R2_SYNC_VERSION = '0.2.0'
const TOOL_NAME = 'bazel-r2-sync'
const PID_FILE = '/tmp/bazel-r2-sync.pid'

let watchProcess = null

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

function r2Env() {
  return {
    ...process.env,
    AWS_ACCESS_KEY_ID: config.r2.accessKeyId,
    AWS_SECRET_ACCESS_KEY: config.r2.secretAccessKey,
  }
}

/**
 * R2 から disk_cache ディレクトリに同期 (restore) + watch 開始
 */
export async function restoreDiskCacheFromR2(diskPath) {
  if (!config.r2.enabled) return false

  try {
    const binary = await getR2SyncBinary()

    // 1. Restore
    execFileSync(binary, [
      'restore',
      '--endpoint', config.r2.endpoint,
      '--bucket', config.r2.bucket,
      '--prefix', 'disk-cache/',
      '--local-path', diskPath,
      '--concurrency', '64',
    ], {
      env: r2Env(),
      stdio: 'inherit',
      timeout: 300000,
    })

    // 2. Start watch in background (uploads new files during build)
    core.info('Starting background R2 watch...')
    watchProcess = spawn(binary, [
      'watch',
      '--endpoint', config.r2.endpoint,
      '--bucket', config.r2.bucket,
      '--prefix', 'disk-cache/',
      '--local-path', diskPath,
      '--concurrency', '64',
      '--interval', '5',
      '--pid-file', PID_FILE,
    ], {
      env: r2Env(),
      stdio: 'inherit',
      detached: true,
    })
    watchProcess.unref()
    core.saveState('r2-watch-pid', String(watchProcess.pid))
    core.info(`Background watch started (PID: ${watchProcess.pid})`)

    return true
  } catch (error) {
    core.warning(`Failed to restore disk cache from R2: ${error.message}`)
    return false
  }
}

/**
 * watch プロセスを停止 (SIGTERM → final sweep + manifest 更新)
 * + 念のため save で最終同期
 */
export async function saveDiskCacheToR2(diskPath) {
  if (!config.r2.enabled) return

  // 1. Stop watch process gracefully (triggers final sweep + manifest update)
  const watchPid = core.getState('r2-watch-pid')
  if (watchPid) {
    core.startGroup('Stop R2 watch (final sweep)')
    try {
      process.kill(Number(watchPid), 'SIGTERM')
      // Wait for watch to finish (it does final sweep + manifest update)
      await waitForProcessExit(Number(watchPid), 30000)
      core.info('Watch process stopped')
    } catch (error) {
      core.warning(`Failed to stop watch: ${error.message}`)
    } finally {
      core.endGroup()
    }
  }

  // 2. Final save to catch anything the watch might have missed
  try {
    const binary = await getR2SyncBinary()
    execFileSync(binary, [
      'save',
      '--endpoint', config.r2.endpoint,
      '--bucket', config.r2.bucket,
      '--prefix', 'disk-cache/',
      '--local-path', diskPath,
      '--concurrency', '64',
    ], {
      env: r2Env(),
      stdio: 'inherit',
      timeout: 600000,
    })
  } catch (error) {
    core.warning(`Failed to save disk cache to R2: ${error.message}`)
  }
}

function waitForProcessExit(pid, timeoutMs) {
  return new Promise((resolve, reject) => {
    const start = Date.now()
    const check = () => {
      try {
        process.kill(pid, 0) // check if alive
        if (Date.now() - start > timeoutMs) {
          // Force kill
          try { process.kill(pid, 'SIGKILL') } catch {}
          resolve()
          return
        }
        setTimeout(check, 500)
      } catch {
        // Process exited
        resolve()
      }
    }
    check()
  })
}
