<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import { ElMessage } from "element-plus";
import { api, type Activity, type Reservation } from "./api";

const activities = ref<Activity[]>([]);
const requests = ref<Reservation[]>([]);
const userId = ref("visitor-001");
const selectedId = ref("");
const quantity = ref(1);
const loading = ref(false);
const creating = ref(false);
const showCreate = ref(false);
const error = ref("");
const now = ref(Date.now());
const draft = ref({
  title: "",
  resourceId: "",
  stock: 10,
  limitPerUser: 1,
  holdSeconds: 60,
  queueTimeoutSeconds: 30,
  minutes: 60,
});
let retryRequest: {
  requestId: string;
  activityId: string;
  userId: string;
  quantity: number;
} | null = null;
const retryAvailable = ref(false);
const selected = computed(() =>
  activities.value.find((item) => item.id === selectedId.value),
);
const activeCount = computed(
  () =>
    activities.value.filter(
      (item) =>
        new Date(item.startsAt).getTime() <= now.value &&
        new Date(item.endsAt).getTime() > now.value,
    ).length,
);
const totalAvailable = computed(() =>
  activities.value.reduce((total, item) => total + item.availableStock, 0),
);
const canBuy = computed(
  () =>
    selected.value &&
    selected.value.availableStock > 0 &&
    new Date(selected.value.startsAt).getTime() <= now.value &&
    new Date(selected.value.endsAt).getTime() > now.value,
);
const labels: Record<Reservation["state"], string> = {
  QUEUED: "排队中",
  RESERVED: "待付款",
  CONFIRMED: "已成交",
  RELEASED: "已取消",
  EXPIRED: "已超时",
  REJECTED: "未抢到",
};
const reasons: Record<string, string> = {
  SOLD_OUT: "配额已抢完",
  LIMIT_EXCEEDED: "已达到限购数量",
  QUEUE_TIMEOUT: "排队超时",
  HOLD_TIMEOUT: "付款超时",
  USER_CANCELLED: "主动取消",
  ACTIVITY_CLOSED: "活动未开放",
};

function remaining(item: Reservation) {
  return Math.max(
    0,
    Math.ceil((new Date(item.expiresAt || "").getTime() - now.value) / 1000),
  );
}

async function refresh() {
  try {
    const [items, history] = await Promise.all([
      api<Activity[]>("/activities"),
      api<Reservation[]>(
        "/requests?userId=" + encodeURIComponent(userId.value.trim()),
      ),
    ]);
    activities.value = items;
    requests.value = history;
    if (!selectedId.value && items.length) selectedId.value = items[0].id;
    error.value = "";
  } catch (failure) {
    error.value =
      failure instanceof Error ? failure.message : "暂时无法连接服务";
  }
}

async function createActivity() {
  if (!draft.value.title.trim() || !draft.value.resourceId.trim()) {
    ElMessage.warning("请填写活动名称与资源编号");
    return;
  }
  creating.value = true;
  try {
    const result = await api<Activity>("/activities", {
      ...draft.value,
      startsAt: new Date(Date.now() - 1000).toISOString(),
      endsAt: new Date(Date.now() + draft.value.minutes * 60000).toISOString(),
    });
    selectedId.value = result.id;
    showCreate.value = false;
    ElMessage.success("活动已创建并开始");
    await refresh();
  } catch (failure) {
    ElMessage.error((failure as Error).message);
  } finally {
    creating.value = false;
  }
}

async function buy(retry = false) {
  if (!selected.value || !userId.value.trim()) {
    ElMessage.warning("请选择活动并填写演示用户编号");
    return;
  }
  loading.value = true;
  if (!retry || !retryRequest)
    retryRequest = {
      requestId: crypto.randomUUID(),
      activityId: selected.value.id,
      userId: userId.value.trim(),
      quantity: quantity.value,
    };
  try {
    await api<Reservation>("/requests", retryRequest);
    retryAvailable.value = false;
    ElMessage.success("请求已受理，请查看抢购记录");
    await refresh();
  } catch (failure) {
    retryAvailable.value = true;
    ElMessage.error((failure as Error).message);
  } finally {
    loading.value = false;
  }
}

async function change(item: Reservation, action: "confirm" | "release") {
  try {
    await api(
      "/requests/" + item.requestId + "/" + action,
      action === "confirm" ? { orderId: "demo-order-" + item.requestId } : {},
    );
    ElMessage.success(
      action === "confirm" ? "模拟付款成功，成交已确认" : "请求已取消",
    );
  } catch (failure) {
    ElMessage.error((failure as Error).message);
  }
  await refresh();
}

let pollTimer: ReturnType<typeof setTimeout>;
let clockTimer: ReturnType<typeof setInterval>;
let mounted = true;
async function poll() {
  await refresh();
  if (mounted) pollTimer = setTimeout(poll, document.hidden ? 10000 : 2500);
}
onMounted(() => {
  void poll();
  clockTimer = setInterval(() => {
    now.value = Date.now();
  }, 1000);
});
onUnmounted(() => {
  mounted = false;
  clearTimeout(pollTimer);
  clearInterval(clockTimer);
});
</script>

<template>
  <div class="workspace">
    <aside class="sidebar">
      <a class="brand" href="/" aria-label="限时首页"
        ><span class="brand-icon">限</span
        ><span>限时<span class="brand-en">FLASH SALE</span></span></a
      >
      <div class="nav-label">工作空间</div>
      <div class="nav-item active"><span>◈</span> 秒杀工作台</div>
      <div class="sidebar-note">
        <span class="status-dot"></span> 本地演示环境
        <p>商品、票务与预约名额<br />共用一套秒杀能力</p>
      </div>
      <div class="sidebar-bottom">
        统一接入 · 按需扩展<span>VERSION 0.1</span>
      </div>
    </aside>
    <main>
      <header class="topbar">
        <span>工作空间 <span class="breadcrumb">/ 秒杀工作台</span></span
        ><span class="environment">DEMO · 模拟支付</span>
      </header>
      <section class="content">
        <div class="page-heading">
          <div>
            <p class="eyebrow">CAMPAIGN WORKSPACE</p>
            <h1>让每一份机会，有序抵达。</h1>
            <p class="subtitle">
              创建活动、体验抢购，查看从排队到成交的完整过程。
            </p>
          </div>
          <el-button type="primary" size="large" @click="showCreate = true"
            >＋ 创建活动</el-button
          >
        </div>
        <el-alert
          v-if="error"
          :title="error"
          type="error"
          :closable="false"
          show-icon
          class="connection-error"
        />
        <div class="stats">
          <article>
            <div class="stat-top">
              <span>活动总数</span><span class="stat-icon">◈</span>
            </div>
            <strong>{{ activities.length }}</strong>
            <p>当前演示业务下的活动</p>
          </article>
          <article>
            <div class="stat-top">
              <span>进行中</span><span class="stat-icon">↗</span>
            </div>
            <strong>{{ activeCount }}<small>场</small></strong>
            <p><span class="status-dot"></span> 开放抢购中的活动</p>
          </article>
          <article>
            <div class="stat-top">
              <span>剩余配额</span><span class="stat-icon">▦</span>
            </div>
            <strong
              >{{ totalAvailable.toLocaleString() }}<small>份</small></strong
            >
            <p>各活动当前可用配额合计</p>
          </article>
        </div>
        <div class="work-grid">
          <section class="panel campaigns">
            <div class="panel-heading">
              <div>
                <h2>活动一览</h2>
                <p>选择一场活动，开始体验</p>
              </div>
              <span class="count">{{ activities.length }} 场活动</span>
            </div>
            <div v-if="!activities.length" class="empty">
              <span>◈</span>
              <h3>第一场活动，从这里开始</h3>
              <p>为商品、票档或预约名额分配独立配额。</p>
              <el-button @click="showCreate = true">创建第一场活动</el-button>
            </div>
            <button
              v-for="item in activities"
              :key="item.id"
              class="campaign-card"
              :class="{ selected: item.id === selectedId }"
              @click="
                selectedId = item.id;
                quantity = 1;
              "
            >
              <div class="campaign-title">
                <span class="resource-icon">{{
                  item.resourceId.startsWith("ticket") ? "票" : "物"
                }}</span>
                <div>
                  <h3>{{ item.title }}</h3>
                  <p>{{ item.resourceId }}</p>
                </div>
                <span
                  class="pill"
                  :class="{ muted: new Date(item.endsAt).getTime() <= now }"
                  >{{
                    new Date(item.endsAt).getTime() <= now
                      ? "已结束"
                      : new Date(item.startsAt).getTime() > now
                        ? "未开始"
                        : "进行中"
                  }}</span
                >
              </div>
              <div class="stock-line">
                <span
                  >剩余 <b>{{ item.availableStock }}</b> /
                  {{ item.stock }}</span
                ><span>每人限 {{ item.limitPerUser }} 份</span>
              </div>
              <div class="progress-track">
                <div
                  :style="{
                    width: (item.availableStock / item.stock) * 100 + '%',
                  }"
                ></div>
              </div>
            </button>
          </section>
          <section class="panel purchase">
            <div class="panel-heading">
              <div>
                <h2>抢购体验</h2>
                <p>使用不同演示用户验证限购</p>
              </div>
              <span class="step">01 — 03</span>
            </div>
            <div class="selected-summary">
              <span>当前选择</span>
              <h3>{{ selected?.title || "请先创建并选择活动" }}</h3>
              <p v-if="selected">
                预占后 {{ selected.holdSeconds }} 秒内完成付款
              </p>
            </div>
            <label class="field-label" for="user-id">演示用户编号</label
            ><el-input
              id="user-id"
              v-model="userId"
              maxlength="128"
              placeholder="例如 visitor-001"
              @change="refresh"
            />
            <label class="field-label" for="quantity">抢购数量</label
            ><el-input-number
              id="quantity"
              v-model="quantity"
              :min="1"
              :max="selected?.limitPerUser || 1"
            />
            <el-button
              class="buy-button"
              type="primary"
              size="large"
              :disabled="!canBuy || !userId.trim()"
              :loading="loading"
              @click="buy(false)"
              >提交抢购 <span>↗</span></el-button
            >
            <el-button
              v-if="retryAvailable"
              class="retry-button"
              :loading="loading"
              @click="buy(true)"
              >使用原编号重试</el-button
            >
            <p class="purchase-note">
              受理后先显示排队结果，获得预占后可模拟付款。取消或超时将归还配额。
            </p>
          </section>
        </div>
        <section class="panel history">
          <div class="panel-heading">
            <div>
              <h2>我的抢购记录</h2>
              <p>用户 {{ userId || "—" }} 的最近 30 条记录 · 自动更新</p>
            </div>
            <el-button text @click="refresh">刷新 ↻</el-button>
          </div>
          <el-table
            :data="requests"
            empty-text="还没有抢购记录，选一场活动试试吧"
            style="width: 100%"
          >
            <el-table-column label="活动" min-width="180"
              ><template #default="{ row }"
                ><span class="table-title">{{
                  activities.find((item) => item.id === row.activityId)
                    ?.title || row.activityId
                }}</span
                ><span class="request-id"
                  >{{ row.requestId.slice(0, 8) }}…</span
                ></template
              ></el-table-column
            >
            <el-table-column prop="quantity" label="数量" width="70" />
            <el-table-column label="状态" min-width="150"
              ><template #default="{ row }"
                ><span class="request-state" :class="row.state.toLowerCase()">{{
                  labels[row.state as Reservation["state"]]
                }}</span
                ><span v-if="row.reason" class="request-id">{{
                  reasons[row.reason] || row.reason
                }}</span></template
              ></el-table-column
            >
            <el-table-column label="付款剩余" width="120"
              ><template #default="{ row }">{{
                row.state === "RESERVED" ? remaining(row) + " 秒" : "—"
              }}</template></el-table-column
            >
            <el-table-column label="操作" min-width="200" align="right"
              ><template #default="{ row }"
                ><el-button
                  v-if="row.state === 'RESERVED'"
                  type="primary"
                  plain
                  size="small"
                  @click="change(row, 'confirm')"
                  >模拟付款</el-button
                ><el-button
                  v-if="['QUEUED', 'RESERVED'].includes(row.state)"
                  size="small"
                  @click="change(row, 'release')"
                  >取消</el-button
                ><span v-else-if="row.state !== 'RESERVED'" class="finished"
                  >已完成</span
                ></template
              ></el-table-column
            >
          </el-table>
        </section>
        <footer>
          本地演示使用模拟身份与模拟支付。真实接入由业务后端完成登录、订单和支付核验。
        </footer>
      </section>
    </main>
    <el-dialog
      v-model="showCreate"
      title="创建秒杀活动"
      width="min(560px, 92vw)"
    >
      <el-form label-position="top" @submit.prevent="createActivity"
        ><el-form-item label="活动名称"
          ><el-input
            v-model="draft.title"
            maxlength="120"
            placeholder="例如 周末音乐会 · 普通票" /></el-form-item
        ><el-form-item label="业务资源编号"
          ><el-input
            v-model="draft.resourceId"
            maxlength="128"
            placeholder="例如 ticket-weekend-standard"
        /></el-form-item>
        <div class="form-grid">
          <el-form-item label="活动配额"
            ><el-input-number
              v-model="draft.stock"
              :min="1"
              :max="1000000" /></el-form-item
          ><el-form-item label="每人限购"
            ><el-input-number
              v-model="draft.limitPerUser"
              :min="1"
              :max="Math.min(draft.stock, 10000)" /></el-form-item
          ><el-form-item label="付款期限（秒）"
            ><el-input-number
              v-model="draft.holdSeconds"
              :min="1"
              :max="86400" /></el-form-item
          ><el-form-item label="最长排队（秒）"
            ><el-input-number
              v-model="draft.queueTimeoutSeconds"
              :min="1"
              :max="3600" /></el-form-item
          ><el-form-item label="活动持续（分钟）"
            ><el-input-number v-model="draft.minutes" :min="1" :max="10080"
          /></el-form-item>
        </div>
        <p class="dialog-note">
          创建后立即开始。接入前请先在原业务系统中划拨活动专用配额。
        </p></el-form
      >
      <template #footer
        ><el-button @click="showCreate = false">取消</el-button
        ><el-button type="primary" :loading="creating" @click="createActivity"
          >创建并开始</el-button
        ></template
      >
    </el-dialog>
  </div>
</template>
