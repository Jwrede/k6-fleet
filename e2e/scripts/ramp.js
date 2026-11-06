// Stages are split, and the VU gauge has to add up across agents.
import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
  stages: [
    { duration: '2s', target: 9 },
    { duration: '3s', target: 9 },
    { duration: '1s', target: 0 },
  ],
};

export default function () {
  http.get(`${__ENV.BASE}/ok`);
  sleep(0.1);
}
