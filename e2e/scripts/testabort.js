// exec.test.abort() ends one instance; the fleet has to end with it.
import http from 'k6/http';
import { sleep } from 'k6';
import exec from 'k6/execution';

export const options = {
  scenarios: { s: { executor: 'constant-vus', vus: 3, duration: '60s' } },
};

export default function () {
  http.get(`${__ENV.BASE}/ok`);
  if (exec.scenario.iterationInTest === 20) {
    exec.test.abort('stopped by the script');
  }
  sleep(0.05);
}
