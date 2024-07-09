
#!/bin/bash
openssl enc -aes-256-ctr -pass pass:default_seed -nosalt < /dev/zero | dd of=rand-GiB10.unsorted bs=1M status=progress iflag=fullblock count=10240
