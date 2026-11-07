// Used from the web page. Long enough that only the Stop button ends it.
import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  vus: 2,
  duration: '60s',
  thresholds: { 'http_req_duration{name:hit}': ['p(95)<5000'] },
};

export function setup() {
  return { token: http.get(`${__ENV.BASE}/setup`).body };
}

export default function () {
  const r = http.get(`${__ENV.BASE}/ok`, { tags: { name: 'hit' } });
  check(r, { 'status is 200': (x) => x.status === 200 });
  if (__ENV.FAIL) {
    http.get(`${__ENV.BASE}/fail`, { tags: { name: 'broken' } });
  }
  if (__ITER === 0) {
    console.log('first iteration done');
    if (__ENV.FAIL) {
      throw new Error('deliberate failure');
    }
  }
  sleep(0.05);
}

export function teardown(data) {
  http.get(`${__ENV.BASE}/teardown?token=${data.token}`);
}
