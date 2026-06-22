import { test } from 'node:test';
import assert from 'node:assert/strict';

const URL = 'http://localhost:3000';

test('login with correct credentials returns a token', async () => {
    const res = await fetch(`${URL}/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: 'admin', password: 'password123' })
    });
    const data = await res.json();
    assert.strictEqual(res.status, 200);
    assert.ok(data.token);
});

test('login with wrong password returns 401', async () => {
    const res = await fetch(`${URL}/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: 'admin', password: 'wrong' })
    });
    assert.strictEqual(res.status, 401);
});

test('profile without token returns 401', async () => {
    const res = await fetch(`${URL}/profile`);
    assert.strictEqual(res.status, 401);
});
