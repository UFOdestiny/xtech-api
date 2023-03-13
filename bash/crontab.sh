#every morning
0 9 * * * /bin/sh /root/api/bash/OpContractInfo.sh

#every minute
* 9,10,11,13,14,15 * * 1-5 /bin/sh /root/api/bash/OpTargetQuote.sh

* 9,10,11,13,14,15 * * 1-5 /bin/sh /root/api/bash/OpDiscount.sh

*/10 9,10,11,13,14,15 * * 1-5 /bin/sh /root/api/bash/OpContractQuote.sh

#every hour
5 9,10,11,13,14,19 * * 1-5 /bin/sh /root/api/bash/OpTargetDerivativeVol.sh

5 9,10,11,13,14,19 * * 1-5 /bin/sh /root/api/bash/OpTargetDerivativePrice.sh

0 12,16 * * 1-5 /bin/sh /root/api/bash/OpNominalAmount.sh

0 12,16 * * 1-5 /bin/sh /root/api/bash/CPR.sh

#after day
30 16 * * 1-5 /bin/sh /root/api/bash/OpContractQuoteALL.sh

30 20 * * 1-5 /bin/sh /root/api/bash/PutdMinusCalld.sh

0 22 * * 1-5 /bin/sh /root/api/bash/OpDiscountALL.sh

0 23 * * 1-5 /bin/sh /root/api/bash/RedisClear.sh