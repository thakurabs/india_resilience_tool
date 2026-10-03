import { createServer } from 'node:http';

const HTML = `<!doctype html><html><body>
<aside><div data-role="user">Duplicate sidebar prompt must not count</div></aside>
<main data-testid="active-conversation" data-conversation-id="sim-conversation"><div data-role="user">Welcome</div></main>
<textarea aria-label="Prompt"></textarea><button type="submit">Send</button>
<div data-testid="quota">10 / 10</div><input type="file">
<script>
globalThis.__cravisDriverSendActivations = 0;
const button = document.querySelector('button'); const input = document.querySelector('textarea');
button.addEventListener('click', () => {
  globalThis.__cravisDriverSendActivations++;
  const main = document.querySelector('main'); const user = document.createElement('div'); user.dataset.role='user'; user.textContent=input.value; main.append(user);
  document.querySelector('[data-testid=quota]').textContent='9 / 10'; button.textContent='Stop'; button.setAttribute('aria-busy','true');
  const answer=document.createElement('div'); answer.dataset.role='assistant'; answer.textContent='Working'; main.append(answer);
  setTimeout(()=>{ answer.innerHTML='Substantive simulated CRAVIS response with stable content.<table><tr><td>1</td></tr></table><a download href="data:text/csv,x">Download</a>'; button.textContent='Send'; button.removeAttribute('aria-busy'); }, 100);
});
</script></body></html>`;

export async function startSimulator() {
  const server = createServer((request, response) => {
    if (request.url === '/favicon.ico') { response.writeHead(204); response.end(); return; }
    response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' }); response.end(HTML);
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  const address = server.address();
  return { origin: `http://127.0.0.1:${address.port}`, close: () => new Promise((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve());
    server.closeAllConnections?.();
  }) };
}
