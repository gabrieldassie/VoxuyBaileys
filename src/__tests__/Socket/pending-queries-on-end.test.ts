import { EventEmitter } from 'events'
import { jest } from '@jest/globals'
import P from 'pino'

// Same approach as status-ack.test.ts: the websocket client is mocked with
// unstable_mockModule and the socket is imported only afterwards.
const send = jest.fn((_data: unknown, cb?: (err?: Error) => void) => {
	cb?.()
	return true
})

class MockWebSocketClient extends EventEmitter {
	isOpen = true
	isClosed = false
	isClosing = false
	isConnecting = false
	connect() {}
	async close() {}
	send = send
}

jest.unstable_mockModule('../../Socket/Client/websocket', () => ({ WebSocketClient: MockWebSocketClient }))

const { DEFAULT_CONNECTION_CONFIG } = await import('../../Defaults')
const { default: makeWASocket } = await import('../../Socket')
const { makeSession } = await import('../TestUtils/session')

const ping = () => ({
	tag: 'iq',
	attrs: { to: 's.whatsapp.net', type: 'get', xmlns: 'w:p' },
	content: [{ tag: 'ping', attrs: {} }]
})

const settleWithin = async <T>(promise: Promise<T>, ms: number) => {
	let timer: NodeJS.Timeout | undefined
	const outcome = await Promise.race([
		promise.then(
			value => ({ state: 'resolved' as const, value }),
			error => ({ state: 'rejected' as const, error })
		),
		new Promise<{ state: 'pending' }>(resolve => {
			timer = setTimeout(() => resolve({ state: 'pending' }), ms)
		})
	])
	clearTimeout(timer)
	return outcome
}

describe('queries in flight when the socket ends', () => {
	it('are rejected right away instead of waiting for the query timeout', async () => {
		const { state, clear } = await makeSession()
		state.creds.me = { id: '1234567890:1@s.whatsapp.net', name: 'Test User' }
		const logger = P({ level: 'silent' })
		const sock = makeWASocket({ ...DEFAULT_CONNECTION_CONFIG, auth: state, logger })

		// nobody answers this ping; the query timeout is 60s (x1.5 behind a proxy)
		const pending = sock.query(ping(), 60_000)
		const unrelated = sock.query(ping(), 60_000)
		await new Promise(resolve => setTimeout(resolve, 20))

		await sock.end(new Error('stream errored'))

		const [first, second] = await Promise.all([settleWithin(pending, 1000), settleWithin(unrelated, 1000)])
		await clear()

		expect(first.state).toBe('rejected')
		expect(second.state).toBe('rejected')
		if (first.state === 'rejected') {
			expect((first.error as { output?: { statusCode?: number } }).output?.statusCode).toBe(428)
		}
	})

	it('a query that already got its reply is not touched by end()', async () => {
		const { state, clear } = await makeSession()
		state.creds.me = { id: '1234567890:1@s.whatsapp.net', name: 'Test User' }
		const logger = P({ level: 'silent' })
		const sock = makeWASocket({ ...DEFAULT_CONNECTION_CONFIG, auth: state, logger })

		const node = ping() as any
		node.attrs.id = 'ping-1'
		const answered = sock.query(node, 60_000)
		await new Promise(resolve => setTimeout(resolve, 20))
		sock.ws.emit('TAG:ping-1', { tag: 'iq', attrs: { id: 'ping-1', type: 'result' } })

		const outcome = await settleWithin(answered, 1000)
		await sock.end(new Error('done'))
		await clear()

		expect(outcome.state).toBe('resolved')
	})
})
