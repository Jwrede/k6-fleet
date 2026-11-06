// Exact work split, setup data, teardown, checks in groups, submetric threshold.
import http from 'k6/http';
import { check, group } from 'k6';
import exec from 'k6/execution';
import { Trend } from 'k6/metrics';
import { hitPath } from './lib/path.js';

// A relative import and open() have to survive the trip through the archive.
const expected = JSON.parse(open('./data/expected.json')).expected;

const deterministic = new Trend('deterministic');

export const options = {
  scenarios: {
    s: { executor: 'shared-iterations', vus: 6, iterations: 1000, maxDuration: '60s' },
  },
  thresholds: {
    deterministic: ['p(95)>940', 'p(95)<960', 'avg==499.5', 'max==999', 'min==0'],
    checks: ['rate==1'],
    'http_req_duration{name:hit}': ['max<5000'],
  },
};

export function setup() {
  return { token: http.post(`${__ENV.BASE}/setup`).body };
}

export default function (data) {
  const i = exec.scenario.iterationInTest;
  deterministic.add(i);
  const r = http.get(`${__ENV.BASE}${hitPath(i, data.token)}`, { tags: { name: 'hit' } });
  check(r, { 'status is 200': (x) => x.status === 200 });
  group('g', () => {
    check(r, { 'body is ok': (x) => x.body === expected });
  });
}

export function teardown(data) {
  http.post(`${__ENV.BASE}/teardown?token=${data.token}`);
}
