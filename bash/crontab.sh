#every morning
25 9 * * * /bin/sh /root/api/bash/OpContractInfo.sh

#every minute
* 9,10,11,13,14 * * 1-5 /bin/sh /root/api/bash/OpTargetQuote.sh

* 9,10,11,13,14 * * 1-5 /bin/sh /root/api/bash/OpContractQuote.sh

* 9,10,11,13,14 * * 1-5 /bin/sh /root/api/bash/OpDiscount.sh

#every hour
10 9,10,11,13,14 * * 1-5 /bin/sh /root/api/bash/OpTargetDerivativeVol.sh

45 9,10,11,13,14 * * 1-5 /bin/sh /root/api/bash/OpNominalAmount.sh

#after day
30 16 * * 1-5 /bin/sh /root/api/bash/OpContractQuoteALL.sh

30 19 * * 1-5 /bin/sh /root/api/bash/PutdMinusCalld.sh
