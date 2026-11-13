import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const accepted = new Counter('sale_accepted');
const throttled = new Counter('sale_throttled');
const unexpected = new Rate('sale_unexpected');
const acceptedDuration = new Trend('sale_accepted_duration', true);
const submissions = new Counter('sale_submissions');
const preallocated = new Counter('sale_preallocated');
const rejected = new Counter('sale_rejected');
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
  const userCount = Number(__ENV.USER_COUNT || 0);
  const userId = userCount > 0 ? `k6-user-${__ITER % userCount}` : `k6-${__VU}-${__ITER}`;
  const body = { requestId: uuid(), activityId: __ENV.ACTIVITY_ID, userId, quantity: 1 };
  const result = http.post((__ENV.BASE_URL || 'http://127.0.0.1:18080') + '/api/v1/requests', JSON.stringify(body), {
    headers: { Authorization: __ENV.AUTHORIZATION, 'Content-Type': 'application/json' },
    timeout: '5s',
  });
  check(result, { 'accepted or explicitly throttled': response => response.status === 202 || response.status === 429 });
  submissions.add(1);
  unexpected.add(result.status !== 202 && result.status !== 429);
  if (result.status === 202) {
    accepted.add(1);
    acceptedDuration.add(result.timings.duration);
    const reservation = result.json();
    check(reservation, { 'response matches request': response => response.requestId === body.requestId });
    check(reservation, { 'new request is queued or rejected': response => response.state === 'QUEUED' || response.state === 'REJECTED' });
    if (reservation.state === 'QUEUED') preallocated.add(1);
    if (reservation.state === 'REJECTED') rejected.add(1);
    if (__ENV.MIXED_READS === 'true') {
      const headers = { Authorization: __ENV.AUTHORIZATION };
      const options = { headers, timeout: '5s', responseCallback: http.expectedStatuses(200) };
      const activity = http.get((__ENV.BASE_URL || 'http://127.0.0.1:18080') + '/api/v1/activities/' + __ENV.ACTIVITY_ID, options);
      const status = http.get((__ENV.BASE_URL || 'http://127.0.0.1:18080') + '/api/v1/requests/' + body.requestId, options);
      check(activity, { 'activity query succeeds': response => response.status === 200 });
      check(status, { 'request query succeeds': response => response.status === 200 });
      unexpected.add(activity.status !== 200);
      unexpected.add(status.status !== 200);
    }
  }
  if (result.status === 429) throttled.add(1);
}
