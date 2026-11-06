// The total rate must be the configured one, not one rate per agent.
import http from 'k6/http';

export const options = {
  scenarios: {
    a: { executor: 'constant-arrival-rate', rate: 60, timeUnit: '1s', duration: '6s', preAllocatedVUs: 12 },
  },
};

export default function () {
  http.get(`${__ENV.BASE}/ok`);
}
