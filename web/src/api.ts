export interface Activity {
  id: string;
  resourceId: string;
  title: string;
  stock: number;
  availableStock: number;
  limitPerUser: number;
  holdSeconds: number;
  queueTimeoutSeconds: number;
  startsAt: string;
  endsAt: string;
}

export interface Reservation {
  requestId: string;
  activityId: string;
  userId: string;
  quantity: number;
  state:
    "QUEUED" | "RESERVED" | "CONFIRMED" | "RELEASED" | "EXPIRED" | "REJECTED";
  reason: string | null;
  expiresAt: string | null;
  createdAt: string;
}

export async function api<T>(path: string, body?: unknown): Promise<T> {
  const response = await fetch("/demo/api" + path, {
    method: body === undefined ? "GET" : "POST",
    headers: body === undefined ? {} : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const result = await response.json();
  if (!response.ok) {
    if (result.state === "EXPIRED") throw new Error("付款期限已过，配额已释放");
    throw new Error(result.message || "操作失败，请稍后重试");
  }
  return result as T;
}
