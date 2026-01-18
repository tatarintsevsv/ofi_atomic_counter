# Atomic counter
An atomic single producer multiple consumer counter implementation using OFI
## Build
```
cmake .
make
```

## Running
Usage: ./ofi_atomic [-h host] [-p provider] [-i iters] [-c clients] [-v]

Where:
* -h - server host (client side only), if not set run as server
* -p - provider name (default psm3)
* -i - iterations count
* -c - clients count to run
* -v - verbose output
* 
### Example
Server side:
`./ofi_atomic -p "udp;ofi_rxd"`

Clent side:
`./ofi_atomic -p "udp;ofi_rxd" -i 100 -v -h 127.0.0.1`

## Notes
Tested on psm3, verbs providers, but have issues on "udp;ofi_rxd" and "tcp;ofi_rxm"
