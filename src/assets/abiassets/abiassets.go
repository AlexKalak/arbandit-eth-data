package abiassets

import _ "embed"

//go:embed events_abi_uniswap_v3.json
var EventsABIUniswapV3String string

//go:embed events_abi_pancakeswap_v3.json
var EventsABIPancakeswapV3String string

//go:embed events_abi_sushiswap_v3.json
var EventsABISushiswapV3String string
