<script lang='ts'>
	import { onMount, onDestroy, getContext, setContext } from 'svelte';
	import L from 'leaflet';

	const { getMap }: { getMap: () => L.Map | undefined } = getContext('map');
	const map = getMap();

    function getCountryBoundariesGeoJson(bounds: L.LatLngBounds) {

        const CountryGeoJsonRequest = new Request("http://0.0.0.0:8080/geoserver/wfs?" + new URLSearchParams({
            service:'wfs',
            version:'2.0.0',
            request:'GetFeature',
            typeName:'world-administrative-boundaries:world-administrative-boundaries',
            propertyName:'name',
            outputformat:'application/json',
            bbox:'45.82944,5.96701,47.80666,10.48821',
            srsName:'EPSG:4326'
        }), {
            headers: {
                'Access-Control-Allow-Origin': '*'
            }
        }
    )

        fetch(CountryGeoJsonRequest).then(response => {
            console.log(response.json())
        })
    }

    onMount(() => {
        map?.addEventListener('moveend', (event) => {
            getCountryBoundariesGeoJson(map.getBounds())
        })
    })

    onDestroy(() => {
        map?.addEventListener('moveend', ()=>{})
    })

</script>

<div>
</div>

<style>

</style>