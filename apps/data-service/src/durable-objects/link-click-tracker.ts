import { deleteClicksBefore, getRecentClicks } from '@/helpers/durable-queries';
import { DurableObject } from 'cloudflare:workers';
import moment from 'moment';

export class LinkClickTracker extends DurableObject<Env> {
	sql: SqlStorage;
	mostRecentOffsetTime = 0;
	leastRecentOffsetTime = 0;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.sql = ctx.storage.sql;

		ctx.blockConcurrencyWhile(async () => {
			const [mostRecentOffsetTime, leastRecentOffsetTime] = await Promise.all([
				this.ctx.storage.get<number>('mostRecentOffsetTime'),
				this.ctx.storage.get<number>('leastRecentOffsetTime'),
			]);

			this.mostRecentOffsetTime = mostRecentOffsetTime || this.mostRecentOffsetTime;
			this.leastRecentOffsetTime = leastRecentOffsetTime || this.leastRecentOffsetTime;

			this.sql.exec(`
        CREATE TABLE IF NOT EXISTS geo_link_clicks (
          latitude REAL NOT NULL,
          longitude REAL NOT NULL,
          country TEXT NOT NULL,
          time INTEGER NOT NULL
        )
      `);
		});
	}

	async addLinkClick(latitude: number, longitude: number, country: string, time: number) {
		this.sql.exec(
			'INSERT INTO geo_link_clicks (latitude, longitude, country, time) VALUES (?, ?, ?, ?)',
			latitude,
			longitude,
			country,
			time
		);

		const alarm = await this.ctx.storage.getAlarm();
		if (!alarm) {
			const oneDay = moment().add(2, 'seconds').valueOf();
			await this.ctx.storage.setAlarm(oneDay);
		}
	}

	async alarm() {
		const clickData = getRecentClicks(this.sql, this.mostRecentOffsetTime);

		const sockets = this.ctx.getWebSockets();
		for (const socket of sockets) {
			socket.send(JSON.stringify(clickData.clicks));
		}

		await this.flushOffsetTimes(clickData.mostRecentTime, clickData.oldestTime);
		deleteClicksBefore(this.sql, clickData.oldestTime);
	}

	async flushOffsetTimes(mostRecentOffsetTime: number, leastRecentOffsetTime: number) {
		this.mostRecentOffsetTime = mostRecentOffsetTime;
		this.leastRecentOffsetTime = leastRecentOffsetTime;
		await this.ctx.storage.put('mostRecentOffsetTime', mostRecentOffsetTime);
		await this.ctx.storage.put('leastRecentOffsetTime', leastRecentOffsetTime);
	}

	async fetch(_: Request) {
		const webSocketPair = new WebSocketPair();
		const [client, server] = Object.values(webSocketPair);

		this.ctx.acceptWebSocket(server);

		return new Response(null, { status: 101, webSocket: client });
	}

	webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean): void | Promise<void> {
		console.log('WebSocket closed', code, reason, wasClean);
	}
}
