<script lang="ts">
    import L from "leaflet";
    import "leaflet/dist/leaflet.css"; 
	import { onDestroy, onMount, setContext } from "svelte";

    export let bounds: L.LatLngBoundsExpression | undefined = undefined;
    export let view: L.LatLngExpression | undefined = undefined;
    export let zoom: number | undefined = undefined;

    let map: L.Map | undefined;
    let mapElement: HTMLDivElement;
    
    onMount(() => {
        map = L.map(mapElement)

        L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        }).addTo(map);

    })

    onDestroy(() => {
        map?.remove();
        map = undefined;
    })

    setContext("map", {
        getMap: () => map
    })

    $: if (map) {
        if (bounds) {
            map.fitBounds(bounds)
        } else if (view && zoom) {
            map.setView(view, zoom)
        }
    }

</script>

<div class="w-full h-full" bind:this={mapElement}>
    {#if map}
        <slot />
    {/if}
</div>
