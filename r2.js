import { execSync } from 'child_process'
import * as core from '@actions/core'
import config from './config.js'

/**
 * R2 (S3-compatible) を使った disk_cache の sync。
 * tar を使わず、content-addressed ファイルを個別に同期する。
 * aws CLI v2 を使用 (GitHub Actions runner にプリインストール済み)。
 */

function r2Env() {
  return {
    ...process.env,
    AWS_ACCESS_KEY_ID: config.r2.accessKeyId,
    AWS_SECRET_ACCESS_KEY: config.r2.secretAccessKey,
    AWS_DEFAULT_REGION: 'auto',
  }
}

function s3Uri() {
  return `s3://${config.r2.bucket}/disk-cache/`
}

function endpointFlag() {
  return `--endpoint-url=${config.r2.endpoint}`
}

/**
 * R2 から disk_cache ディレクトリに同期 (restore)
 */
export async function restoreDiskCacheFromR2(diskPath) {
  if (!config.r2.enabled) return false

  core.startGroup('Restore disk cache from R2')
  try {
    execSync(
      `aws s3 sync ${s3Uri()} ${diskPath} ${endpointFlag()} --no-progress --only-show-errors`,
      { env: r2Env(), stdio: 'inherit', timeout: 300000 }
    )
    core.info('Successfully restored disk cache from R2')
    return true
  } catch (error) {
    core.warning(`Failed to restore disk cache from R2: ${error.message}`)
    return false
  } finally {
    core.endGroup()
  }
}

/**
 * disk_cache ディレクトリを R2 に同期 (save)
 */
export async function saveDiskCacheToR2(diskPath) {
  if (!config.r2.enabled) return

  core.startGroup('Save disk cache to R2')
  try {
    execSync(
      `aws s3 sync ${diskPath} ${s3Uri()} ${endpointFlag()} --no-progress --only-show-errors`,
      { env: r2Env(), stdio: 'inherit', timeout: 600000 }
    )
    core.info('Successfully saved disk cache to R2')
  } catch (error) {
    core.warning(`Failed to save disk cache to R2: ${error.message}`)
  } finally {
    core.endGroup()
  }
}
