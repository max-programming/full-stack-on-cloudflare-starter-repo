import { getLink } from '@repo/data-ops/queries/links';
import { linkSchema, type LinkSchemaType } from '@repo/data-ops/zod-schema/links';
import type { LinkClickMessageType } from '@repo/data-ops/zod-schema/queue';
import moment from 'moment';

const TTL_TIME = 60 * 60 * 24;

export async function getLinkInfoFromKv(env: Env, id: string) {
	const linkInfo = await env.CACHE.get(id);
	if (!linkInfo) return null;

	try {
		const parsedLinkInfo = JSON.parse(linkInfo);
		return linkSchema.parse(parsedLinkInfo);
	} catch {
		return null;
	}
}

export async function saveLinkInfoToKv(env: Env, id: string, linkInfo: LinkSchemaType) {
	try {
		await env.CACHE.put(id, JSON.stringify(linkInfo), {
			expirationTtl: TTL_TIME,
		});
	} catch (error) {
		console.error('Error saving link info to KV', error);
	}
}

export async function getRoutingDestination(env: Env, id: string) {
	const linkInfo = await getLinkInfoFromKv(env, id);
	if (linkInfo) return linkInfo;

	const linkInfoFromDb = await getLink(id);
	if (!linkInfoFromDb) return null;

	await saveLinkInfoToKv(env, id, linkInfoFromDb);

	return linkInfoFromDb;
}

export function getDestinationForCountry(linkInfo: LinkSchemaType, countryCode?: string) {
	if (!countryCode) {
		return linkInfo.destinations.default;
	}

	if (linkInfo.destinations[countryCode]) {
		return linkInfo.destinations[countryCode];
	}

	return linkInfo.destinations.default;
}

export async function scheduleEvalWorkflow(env: Env, linkInfo: LinkClickMessageType) {
	const doId = env.EVALUATION_SCHEDULER.idFromName(`${linkInfo.data.id}:${linkInfo.data.destination}`);
	const stub = env.EVALUATION_SCHEDULER.get(doId);

	await stub.collectLinkClick(linkInfo.data.accountId, linkInfo.data.id, linkInfo.data.destination, linkInfo.data.country || 'UNKNOWN');
}

export async function captureLinkClickInBackground(env: Env, event: LinkClickMessageType) {
	await env.QUEUE.send(event);

	const doId = env.LINK_CLICK_TRACKER.idFromName(event.data.accountId);
	const stub = env.LINK_CLICK_TRACKER.get(doId);

	if (!event.data.latitude || !event.data.longitude || !event.data.country) {
		return;
	}

	await stub.addLinkClick(event.data.latitude, event.data.longitude, event.data.country, moment().valueOf());
}
