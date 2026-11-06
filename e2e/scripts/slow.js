// Each tenth request is slow, so p(95) is only crossed on the complete data.
import http from 'k6/http';
import exec from 'k6/execution';

export const options = {
  scenarios: {
    s: { executor: 'shared-iterations', vus: 6, iterations: 120, maxDuration: '60s' },
  },
  thresholds: { http_req_duration: ['p(95)<100'] },
};

export default function () {
  const path = exec.scenario.iterationInTest % 10 === 0 ? 'slow' : 'ok';
  http.get(`${__ENV.BASE}/${path}`);
}
