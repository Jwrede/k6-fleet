import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
  scenarios: { s: { executor: 'constant-vus', vus: 3, duration: '60s' } },
  thresholds: { http_req_failed: [{ threshold: 'rate<0.1', abortOnFail: true }] },
};

export default function () {
  http.get(`${__ENV.BASE}/fail`);
  sleep(0.05);
}
