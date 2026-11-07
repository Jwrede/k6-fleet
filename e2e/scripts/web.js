// Long enough that only the Stop button ends it.
import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
  vus: 4,
  duration: '60s',
  thresholds: { http_req_failed: ['rate<0.01'] },
};

export function setup() {
  return { token: http.get(`${__ENV.BASE}/setup`).body };
}

export default function () {
  http.get(`${__ENV.BASE}/ok`);
  sleep(0.05);
}

export function teardown(data) {
  http.get(`${__ENV.BASE}/teardown?token=${data.token}`);
}
