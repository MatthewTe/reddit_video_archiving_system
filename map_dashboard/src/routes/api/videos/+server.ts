import { json } from '@sveltejs/kit';
import { driver, auth, Driver } from "neo4j-driver";
import { NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD } from '$env/static/private';

export async function GET(event) {

	let currentDriver: Driver = driver(NEO4J_URI, auth.basic(NEO4J_USER, NEO4J_PASSWORD))
	const { records, summary, keys } = await currentDriver.executeQuery(`MATCH (n:StaticFile:Video) RETURN n`)

	return json(records)

}