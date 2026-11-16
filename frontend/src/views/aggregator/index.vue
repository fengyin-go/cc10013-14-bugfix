<template>
  <section class="page" data-module="aggregator">
    <header class="page-head">
      <div>
        <h2>数据聚合管理</h2>
        <p class="page-desc">
          维护聚合任务的输入、执行约束与结果记录；失败批次保留待处理标记与已完成片段，
          必需片段齐备前结果集不可消费。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记聚合任务</button>
        <button class="btn" type="button" @click="exportRows">导出数据聚合清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <label class="filter-item">
        <span>状态</span>
        <select v-model="statusFilter">
          <option value="">全部状态</option>
          <option v-for="status in statuses" :key="status" :value="status">{{ status }}</option>
        </select>
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>任务状态</th>
          <th>批次状态</th>
          <th>数据集状态</th>
          <th>归档状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="row in rows"
          :key="String(row.id)"
          class="data-row"
          @click="openBatch(row)"
        >
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status ?? '—' }}</td>
          <td>{{ row['批次状态'] ?? '无批次' }}</td>
          <td>{{ row['数据集状态'] ?? '—' }}</td>
          <td :class="{ 'error-text': row['归档状态'] === '已归档' && row.status === '已失败' }">
            {{ row['归档状态'] ?? '未归档' }}
          </td>
          <td class="row-actions" @click.stop>
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button class="link" type="button" @click="openBatch(row)">批次详情</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 5" class="empty-state">暂无数据聚合数据，可先登记聚合任务</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条数据聚合记录</span>
      <span v-if="message" class="info-text">{{ message }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <div v-if="createOpen" class="drawer-mask" @click.self="createOpen = false">
      <aside class="drawer">
        <header class="drawer-head">
          <h3>登记聚合任务</h3>
          <button class="link" type="button" @click="createOpen = false">关闭</button>
        </header>
        <form class="drawer-body" @submit.prevent="submitCreate">
          <label v-for="field in createFields" :key="field" class="field">
            <span>{{ field }}<em v-if="requiredFields.includes(field)"> *</em></span>
            <input v-model="createValues[field]" :placeholder="field === '去重键' ? '例如 order_id' : field === '输出格式' ? '例如 jsonl / csv / json' : ''" />
          </label>
          <p class="hint">去重键与输出格式是聚合结果的一部分，登记时一并保存。</p>
          <button class="btn primary" type="submit">保存</button>
          <span v-if="createError" class="error-text">{{ createError }}</span>
        </form>
      </aside>
    </div>

    <div v-if="drawerOpen" class="drawer-mask" @click.self="closeDrawer">
      <aside class="drawer wide">
        <header class="drawer-head">
          <div>
            <h3>批次详情 · {{ selectedEntry?.['任务编号'] ?? selectedEntry?.id }}</h3>
            <p class="hint">
              数据集 {{ detail?.entry?.['聚合数据集'] ?? '—' }} ·
              批次 {{ detail?.batch_id ?? selectedEntry?.['批次编号'] ?? '无批次' }}
            </p>
          </div>
          <button class="link" type="button" @click="closeDrawer">关闭</button>
        </header>

        <div class="drawer-body">
          <p v-if="detailError" class="error-text">{{ detailError }}</p>
          <p v-if="detailLoading" class="hint">正在读取批次详情…</p>

          <template v-if="detail && !detailLoading">
            <div class="badge-row">
              <span class="badge" :class="{ bad: detail.dataset_status !== '已完成' }">
                数据集：{{ detail.dataset_status ?? '—' }}
              </span>
              <span class="badge" :class="{ bad: !detail.consumable }">
                {{ detail.consumable ? '结果可消费' : '结果不可消费' }}
              </span>
              <span class="badge" :class="{ bad: selectedEntry?.['归档状态'] === '已归档' && selectedEntry?.status === '已失败' }">
                {{ selectedEntry?.['归档状态'] ?? '未归档' }}
              </span>
            </div>

            <section class="detail-block">
              <h4>失败原因</h4>
              <p v-if="detail.failure_reason" class="error-text">{{ detail.failure_reason }}</p>
              <p v-else class="hint">当前批次没有失败原因。</p>
            </section>

            <section class="detail-block">
              <h4>异常片段</h4>
              <p v-if="detail.abnormal_fragments?.length" class="chip-row">
                <span v-for="fragment in detail.abnormal_fragments" :key="fragment" class="chip bad">{{ fragment }}</span>
              </p>
              <p v-else class="hint">没有异常片段。</p>
            </section>

            <section class="detail-block">
              <h4>片段进度</h4>
              <p>已完成：</p>
              <p v-if="detail.completed_fragments?.length" class="chip-row">
                <span v-for="fragment in detail.completed_fragments" :key="fragment" class="chip">{{ fragment }}</span>
              </p>
              <p v-else class="hint">暂无已完成片段。</p>
              <p>剩余（缺失）：</p>
              <p v-if="detail.remaining_fragments?.length" class="chip-row">
                <span v-for="fragment in detail.remaining_fragments" :key="fragment" class="chip bad">{{ fragment }}</span>
              </p>
              <p v-else class="hint">全部必需片段已齐备。</p>
            </section>

            <section v-if="detail.fragment_details?.length" class="detail-block">
              <h4>片段明细</h4>
              <table class="data-table compact">
                <thead>
                  <tr><th>片段</th><th>状态</th><th>摘要</th><th>来源</th><th>优先级</th><th>版本</th><th>失败原因</th></tr>
                </thead>
                <tbody>
                  <tr v-for="fragment in detail.fragment_details" :key="fragment.fragment_id">
                    <td>{{ fragment.fragment_id }}</td>
                    <td>{{ fragment.status }}</td>
                    <td>{{ fragment.summary ?? '—' }}</td>
                    <td>{{ fragment.source ?? '—' }}</td>
                    <td>{{ fragment.source_priority ?? '—' }}</td>
                    <td>{{ fragment.version ?? '—' }}</td>
                    <td>{{ fragment.failure_reason ?? '—' }}</td>
                  </tr>
                </tbody>
              </table>
            </section>

            <section class="detail-block">
              <h4>可重试输入</h4>
              <pre class="code-block">{{ formatJson(detail.retryable_input) }}</pre>
            </section>

            <section class="detail-block">
              <h4>空态与恢复入口</h4>
              <p v-if="detail.empty_state?.visible" class="hint">{{ detail.empty_state?.message }}</p>
              <p v-else class="hint">{{ detail.recovery?.message }}</p>
              <div v-if="detail.recovery?.available" class="retry-form">
                <label v-for="fragment in detail.remaining_fragments" :key="fragment" class="field">
                  <span>{{ fragment }} 摘要</span>
                  <input v-model="retrySummaries[fragment]" :placeholder="`补齐 ${fragment} 的摘要`" />
                </label>
                <button class="btn primary" type="button" :disabled="retrying" @click="retryBatch">
                  {{ retrying ? '重试中…' : '从缺失片段重试' }}
                </button>
              </div>
              <p v-else class="hint">没有需要恢复的片段，可直接在列表中归档结果。</p>
            </section>

            <section class="detail-block">
              <h4>重试结果</h4>
              <p v-if="retryNextStep" :class="retrySucceeded ? 'info-text' : 'error-text'">{{ retryNextStep }}</p>
              <p v-else class="hint">尚未执行重试。</p>
            </section>

            <section class="detail-block">
              <h4>结果消费门禁</h4>
              <button class="btn" type="button" @click="loadResult">读取聚合结果</button>
              <p v-if="resultMessage" :class="resultConsumable ? 'info-text' : 'error-text'">{{ resultMessage }}</p>
              <pre v-if="resultPreview" class="code-block">{{ resultPreview }}</pre>
            </section>
          </template>
        </div>
      </aside>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { fetchJson, request } from '@/api/client'

type Row = Record<string, string | number | boolean | null | undefined>

type FragmentDetail = {
  fragment_id: string
  status?: string | null
  summary?: string | null
  source?: string | null
  source_priority?: number | null
  version?: number | null
  failure_reason?: string | null
}

type BatchDetail = {
  batch_id: string
  failure_reason: string | null
  abnormal_fragments: string[]
  completed_fragments: string[]
  remaining_fragments: string[]
  fragment_details: FragmentDetail[]
  retryable_input: Record<string, unknown>
  empty_state?: { visible: boolean; message: string }
  recovery?: { available: boolean; action: string; message: string }
  next_step?: string
  dataset_status?: string
  consumable?: boolean
  entry?: Row
}

const ENDPOINT = '/api/aggregator'
const columns = ["任务编号", "输入端点列表", "合并策略", "去重键", "排序字段", "输出格式", "批次大小", "调度周期"]
const actions = ["提交执行", "启动聚合", "终止聚合", "归档结果"]
const statuses = ["草稿", "待执行", "运行中", "已完成", "已失败"]
const createFields = ["任务编号", "输入端点列表", "合并策略", "去重键", "排序字段", "输出格式", "批次大小", "调度周期"]
const requiredFields = ["任务编号", "输入端点列表", "合并策略"]

const rows = ref<Row[]>([])
const total = ref(0)
const message = ref('')
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const statusFilter = ref('')
const filterFields = columns.slice(0, 3)

const createOpen = ref(false)
const createValues = ref<Record<string, string>>({})
const createError = ref('')

const drawerOpen = ref(false)
const detailLoading = ref(false)
const detailError = ref('')
const selectedEntry = ref<Row | null>(null)
const detail = ref<BatchDetail | null>(null)
const retrySummaries = ref<Record<string, string>>({})
const retrying = ref(false)
const retryNextStep = ref('')
const retrySucceeded = ref(false)
const resultMessage = ref('')
const resultConsumable = ref(false)
const resultPreview = ref('')

const stats = computed(() => [
  { label: '聚合任务总数', value: rows.value.length },
  { label: '运行中任务', value: rows.value.filter((row) => row.status === '运行中').length },
  { label: '失败待处理', value: rows.value.filter((row) => row.status === '已失败' && row.pending).length },
  { label: '可消费数据集', value: rows.value.filter((row) => row['可消费'] === true).length },
])

function resetFilters() {
  filters.value = {}
  statusFilter.value = ''
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  createOpen.value = true
  createError.value = ''
  createValues.value = Object.fromEntries(createFields.map((field) => [field, '']))
}

async function submitCreate() {
  createError.value = ''
  const missing = requiredFields.filter((field) => !String(createValues.value[field] ?? '').trim())
  if (missing.length) {
    createError.value = `缺少必填字段：${missing.join('、')}`
    return
  }
  try {
    const response = await request(ENDPOINT, {
      method: 'POST',
      body: JSON.stringify({ values: createValues.value }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) {
      throw new Error(payload.message || '聚合任务登记失败')
    }
    createOpen.value = false
    message.value = payload.message
    await reload()
  } catch (error) {
    createError.value = error instanceof Error ? error.message : '聚合任务登记失败'
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  message.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) {
      throw new Error(payload.message || payload.detail || '数据聚合动作未生效，请稍后重试')
    }
    message.value = payload.message
    await reload()
    if (drawerOpen.value && selectedEntry.value?.id === row.id) {
      await openBatch(row)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据聚合操作失败'
  }
}

async function openBatch(row: Row, keepFeedback = false) {
  const batchId = String(row['批次编号'] || 'latest')
  drawerOpen.value = true
  selectedEntry.value = row
  detailError.value = ''
  detailLoading.value = true
  detail.value = null
  retrySummaries.value = {}
  if (!keepFeedback) {
    retryNextStep.value = ''
    retrySucceeded.value = false
    resultMessage.value = ''
    resultConsumable.value = false
    resultPreview.value = ''
  }
  try {
    const payload = await fetchJson<BatchDetail>(
      `${ENDPOINT}/${row.id}/batches/${encodeURIComponent(batchId)}`,
    )
    detail.value = payload
    selectedEntry.value = payload.entry ?? row
  } catch (error) {
    detailError.value = error instanceof Error ? error.message : '批次详情读取失败'
  } finally {
    detailLoading.value = false
  }
}

function closeDrawer() {
  drawerOpen.value = false
  detail.value = null
  selectedEntry.value = null
}

async function retryBatch() {
  if (!selectedEntry.value || !detail.value) return
  retrying.value = true
  retryNextStep.value = ''
  try {
    const fragments = Object.entries(retrySummaries.value)
      .filter(([, summary]) => summary.trim())
      .map(([fragment_id, summary]) => ({ fragment_id, summary: summary.trim() }))
    const response = await request(
      `${ENDPOINT}/${selectedEntry.value.id}/batches/${encodeURIComponent(detail.value.batch_id)}/retry`,
      { method: 'POST', body: JSON.stringify({ values: { fragments } }) },
    )
    const payload = await response.json()
    retrySucceeded.value = Boolean(payload.ok)
    retryNextStep.value = payload.next_step || payload.message || '重试已提交'
    await reload()
    const refreshed = rows.value.find((item) => item.id === selectedEntry.value?.id)
    if (refreshed) {
      await openBatch(refreshed, true)
    }
  } catch (error) {
    retrySucceeded.value = false
    retryNextStep.value = error instanceof Error ? error.message : '批次重试失败'
  } finally {
    retrying.value = false
  }
}

async function loadResult() {
  if (!selectedEntry.value) return
  resultMessage.value = ''
  resultPreview.value = ''
  try {
    const payload = await fetchJson<Record<string, unknown>>(`${ENDPOINT}/${selectedEntry.value.id}/result`)
    resultConsumable.value = Boolean(payload.consumable)
    resultMessage.value = String(payload.message ?? '')
    resultPreview.value = payload.consumable ? formatJson(payload.items) : ''
  } catch (error) {
    resultConsumable.value = false
    resultMessage.value = error instanceof Error ? error.message : '聚合结果读取失败'
  }
}

function formatJson(value: unknown) {
  const empty = !value || (typeof value === 'object' && !Array.isArray(value) && !Object.keys(value as object).length)
  return empty ? '无' : JSON.stringify(value, null, 2)
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>)
  if (statusFilter.value) {
    query.set('status', statusFilter.value)
  }
  try {
    const response = await request(`${ENDPOINT}?${query.toString()}`)
    if (!response.ok) {
      throw new Error('聚合任务列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据聚合列表读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.data-row { cursor: pointer; }
.data-row:hover { background: #f8fafc; }
.info-text { color: #0f766e; }
.hint { color: var(--muted); font-size: 12px; }
.drawer-mask {
  position: fixed;
  inset: 0;
  background: rgb(15 23 42 / 35%);
  display: flex;
  justify-content: flex-end;
  z-index: 20;
}
.drawer {
  width: 460px;
  max-width: 100%;
  background: #fff;
  height: 100%;
  display: flex;
  flex-direction: column;
  box-shadow: -8px 0 24px rgb(15 23 42 / 18%);
}
.drawer.wide { width: 760px; }
.drawer-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  padding: 14px 18px;
  border-bottom: 1px solid var(--border);
}
.drawer-head h3 { margin: 0 0 4px; font-size: 15px; }
.drawer-body { padding: 16px 18px; overflow-y: auto; }
.field { display: block; margin-bottom: 10px; }
.field span { display: block; font-size: 12px; color: var(--muted); margin-bottom: 4px; }
.field input {
  width: 100%;
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 6px 8px;
  font-size: 13px;
}
.badge-row { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 12px; }
.badge {
  border: 1px solid #bbf7d0;
  background: #f0fdf4;
  color: #166534;
  border-radius: 999px;
  padding: 2px 10px;
  font-size: 12px;
}
.badge.bad { border-color: #fecaca; background: #fef2f2; color: #b42318; }
.detail-block { border-top: 1px solid var(--border); padding: 10px 0; }
.detail-block h4 { margin: 0 0 8px; font-size: 13px; }
.chip-row { display: flex; flex-wrap: wrap; gap: 6px; margin: 4px 0 8px; }
.chip {
  border-radius: 4px;
  background: #eff6ff;
  color: #1d4ed8;
  padding: 2px 8px;
  font-size: 12px;
}
.chip.bad { background: #fef2f2; color: #b42318; }
.code-block {
  background: #0f172a;
  color: #e2e8f0;
  border-radius: 6px;
  padding: 10px;
  font-size: 12px;
  max-height: 220px;
  overflow: auto;
}
.data-table.compact th,
.data-table.compact td { padding: 5px 6px; font-size: 12px; }
.retry-form { margin-top: 10px; }
</style>
