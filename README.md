# USFTP
Ultra-speed File Transfer Protocol

## DiamondGotCat's Protocols

### UltraDP: Most Fast File Transfer Protocol in My Projects
8 Gbps(1 GB/s) in My Wi-fi

### NextDP(NextDrop): You can use Official Python Library
4.8 Gbps(0.6 GB/s) in My Wi-fi

### USFTP: Built-in File integrity check function (SHA-256)
2 Gbps(0.25 GB/s) in My Wi-fi

## Infomation
- Protocol Type: TCP
- Default Port: 28380

## Dependencies
- `hashlib`
- `rich`

## Command Examples
- `python3 main.py --mode drop --role receive`
- `python3 main.py --mode drop --role send --file example.zip --target_ip my-pc.local`
