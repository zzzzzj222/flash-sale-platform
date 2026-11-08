interface SaleRequest {
  requestId: string;
  activityId: string;
  quantity: number;
}

interface SaleResult {
  requestId: string;
  state: 'QUEUED' | 'RESERVED' | 'CONFIRMED' | 'RELEASED' | 'EXPIRED' | 'REJECTED';
  expiresAt: string | null;
  reason: string | null;
}

export function createSaleApi(baseUrl: string, userToken: () => string) {
  if (!/^https:\/\/[^/]+/.test(baseUrl)) throw new Error('请配置业务后端 HTTPS 域名');
  const base = baseUrl.replace(/\/$/, '');

  function request<T extends string | ArrayBuffer | WechatMiniprogram.IAnyObject>(path: string, method: 'GET' | 'POST', data?: unknown): Promise<T> {
    return new Promise((resolve, reject) => {
      wx.request<T>({
        url: base + '/sale' + path,
        method,
        data: data as WechatMiniprogram.IAnyObject,
        timeout: 10000,
        header: { Authorization: 'Bearer ' + userToken(), 'Content-Type': 'application/json' },
        success(response) {
          if (response.statusCode >= 200 && response.statusCode < 300) resolve(response.data);
          else reject(new Error('请求失败，状态码 ' + response.statusCode));
        },
        fail(error) { reject(new Error(error.errMsg)); },
      });
    });
  }

  return {
    activities: () => request<unknown[]>('/activities', 'GET'),
    submit: (input: SaleRequest) => request<SaleResult>('/requests', 'POST', input),
    result: (requestId: string) => request<SaleResult>('/requests/' + encodeURIComponent(requestId), 'GET'),
    cancel: (requestId: string) => request<SaleResult>('/requests/' + encodeURIComponent(requestId) + '/cancel', 'POST', {}),
  };
}
