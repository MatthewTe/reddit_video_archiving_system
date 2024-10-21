<script lang='ts'>
    import "../app.css"
    import type { LatLngExpression } from "leaflet";
    import Leaflet from "$lib/Leaflet.svelte";
    import GeoJsonLayer from "$lib/GeoJsonLayer.svelte";

    const initalView: LatLngExpression = [51.51, 7.46]

    const markerLocation: LatLngExpression = [51.513870009926, 7.473969975241]
    


</script>
<div class="w-full h-screen">
    <Leaflet view={initalView} zoom={13}>
        <GeoJsonLayer></GeoJsonLayer>
   </Leaflet>
</div>

