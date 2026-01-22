import { durableObjectGeoClickArraySchema } from '@repo/data-ops/zod-schema/links';

export function getRecentClicks(sqlStorage: SqlStorage, offsetTime = 0, limit = 50) {
	const cursor = sqlStorage.exec(
		'SELECT latitude, longitude, country, time FROM geo_link_clicks WHERE time > ? ORDER BY time DESC LIMIT ?',
		offsetTime,
		limit
	);

	const clicks = durableObjectGeoClickArraySchema.parse(cursor.toArray());
	const mostRecentTime = clicks.length > 0 ? clicks[0].time : 0;
	const oldestTime = clicks.length > 0 ? clicks[clicks.length - 1].time : 0;

	return {
		clicks,
		mostRecentTime,
		oldestTime,
	};
}

export function deleteClicksBefore(sqlStorage: SqlStorage, time: number) {
	sqlStorage.exec('DELETE FROM geo_link_clicks WHERE time < ?', time);
}
