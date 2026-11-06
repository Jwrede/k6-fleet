import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    checkout: { executor: 'constant-arrival-rate', rate: 3000, timeUnit: '1s', duration: '5m', preAllocatedVUs: 600 },
  },
  thresholds: {
    http_req_duration: ['p(95)<300'],
    http_req_failed: [{ threshold: 'rate<0.01', abortOnFail: true, delayAbortEval: '30s' }],
  },
};

export function setup() {
  return { token: http.post(`${__ENV.BASE}/login`).json('token') };
}

export default function (data) {
  const r = http.get(`${__ENV.BASE}/cart`, { headers: { Authorization: `Bearer ${data.token}` } });
  check(r, { 'status is 200': (x) => x.status === 200 });
}
