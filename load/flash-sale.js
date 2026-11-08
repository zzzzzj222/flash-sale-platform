import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const accepted = new Counter('sale_accepted');
const throttled = new Counter('sale_throttled');
const unexpected = new Rate('sale_unexpected');
const acceptedDuration = new Trend('sale_accepted_duration', true);
http.setResponseCallback(http.expectedStatuses(202, 429));
export const options = {
  scenarios: {
    burst: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 100),
      timeUnit: '1s',
      duration: __ENV.DURATION || '20s',
      preAllocatedVUs: Number(__ENV.VUS || 50),
      maxVUs: Number(__ENV.MAX_VUS || 200),
    },
  },
  thresholds: {
    checks: ['rate>0.99'],
    sale_unexpected: [{ threshold: 'rate<0.05', abortOnFail: true, delayAbortEval: '10s' }],
  },
};

function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, character => {
    const random = Math.floor(Math.random() * 16);
    return (character === 'x' ? random : (random & 3) | 8).toString(16);
  });
}

export default function () {
  if (!__ENV.AUTHORIZATION || !__ENV.ACTIVITY_ID) throw new Error('Set AUTHORIZATION and ACTIVITY_ID');
  const body = { requestId: uuid(), activityId: __ENV.ACTIVITY_ID, userId: `k6-${__VU}-${__ITER}`, quantity: 1 };
  const result = http.post((__ENV.BASE_URL || 'http://127.0.0.1:18080') + '/api/v1/requests', JSON.stringify(body), {
    headers: { Authorization: __ENV.AUTHORIZATION, 'Content-Type': 'application/json' },
    timeout: '5s',
  });
  check(result, { 'accepted or explicitly throttled': response => response.status === 202 || response.status === 429 });
  unexpected.add(result.status !== 202 && result.status !== 429);
  if (result.status === 202) {
    accepted.add(1);
    acceptedDuration.add(result.timings.duration);
  }
  if (result.status === 429) throttled.add(1);
}
