package Tests;

import Sketches.HashFunctions.PairwiseIndependentHashFunctions;
import org.junit.Assert;
import org.junit.Test;
import Sketches.BloomFilter;
import Sketches.CountMinSketch;
import java.lang.Math;

import java.util.ArrayList;
import java.util.BitSet;

public class BloomFilterTest {

    @Test
    public void updateTest(){
        BloomFilter bloomFilter= new BloomFilter( 800, 5000,200);
        Assert.assertEquals(bloomFilter.getnHashFunctions(),4);
        PairwiseIndependentHashFunctions hashFunctions= new PairwiseIndependentHashFunctions(4, 200);
        BitSet hashmap = bloomFilter.getHashmap();
        Assert.assertTrue(hashmap.equals(new BitSet(5000)));

        bloomFilter.update(12);
        BitSet testbitset = bloomFilter.getHashmap();
        Assert.assertEquals(testbitset.cardinality(),bloomFilter.getnHashFunctions());
        for (int i = 0; i<800; i++) {
            bloomFilter.update(12);
            Assert.assertTrue(hashmap.equals(testbitset));
        }

        Assert.assertTrue(bloomFilter.getHashmap().equals(testbitset));
    }
    @Test
    public void queryTest(){
        int positivecount=0;
        //int trueNegativecount=0;
        BloomFilter bloomFilter= new BloomFilter( 1500, 3000,200);
        for (int i = 600; i< 2100; i++){
            bloomFilter.update(i);
        }

        for (int j=0;j<2100; j++){
           if(bloomFilter.query(j)) {
               positivecount++;
           }
        }
        /*for (int j=0;j<600; j++){
            if(!bloomFilter.query(j)) {
                trueNegativecount++;
            }
        }*/
        double ratioArgument= Math.exp(-(bloomFilter.getnHashFunctions()*1500)/3000.0);
        double fprateEstimate= Math.pow((1-ratioArgument),bloomFilter.getnHashFunctions());
        double falsePositivecount= positivecount-bloomFilter.getElementsProcessed();
        double falsePositiverate= falsePositivecount/(2100);//1500+trueNegativecount);
        Assert.assertTrue(falsePositiverate<=fprateEstimate);
    }
    @Test(expected = Exception.class)
    public void illegalMergesizeTest() throws Exception {
        BloomFilter bloomFilter= new BloomFilter( 5, 20,200);
        BloomFilter other= new BloomFilter( 7, 18,200);
        bloomFilter.merge(other);
    }
    @Test(expected = Exception.class)
    public void illegalMergeSamplesTest() throws Exception {
        BloomFilter bloomFilter= new BloomFilter( 7, 20,200);
        CountMinSketch other= new CountMinSketch(10,20,(long)200);
        bloomFilter.merge(other);
    }
    @Test
    public void mergeTest() throws Exception{
        BloomFilter bloomFilter= new BloomFilter( 1000, 2000,200);
        for(int i=13000;i<13870;i++){
            bloomFilter.update(i);
        }


        BloomFilter other= new BloomFilter( 950, 2000,200);
        for(int i=1200;i<2000;i++){
            other.update(i);
        }

        int[] intersecresult= new int[]{2, 3, 4, 5, 6, 7, 8, 9, 10, 44, 45, 46, 47, 48, 49, 50, 51, 52, 85, 86, 87, 88, 89, 90, 91, 92, 93, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 335, 336, 337, 338, 339, 340, 341, 342, 343, 377, 378, 379, 380, 381, 382, 383, 384, 385, 418, 419, 420, 421, 422, 423, 424, 425, 426, 427, 460, 461, 462, 463, 464, 465, 466, 467, 468, 502, 503, 504, 505, 506, 507, 508, 509, 510, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552, 584, 585, 586, 587, 588, 589, 590, 591, 592, 593, 594, 627, 628, 629, 630, 631, 632, 633, 634, 635, 669, 670, 671, 672, 673, 674, 675, 676, 677, 710, 711, 712, 713, 714, 715, 716, 717, 718, 719, 751, 752, 753, 754, 755, 756, 757, 758, 759, 760, 793, 794, 795, 796, 797, 798, 799, 800, 801, 835, 836, 837, 838, 839, 840, 841, 842, 843, 844, 876, 877, 878, 879, 880, 881, 882, 883, 884, 885, 886, 918, 919, 920, 921, 922, 923, 924, 925, 926, 927, 960, 961, 962, 963, 964, 965, 966, 967, 968, 969, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050, 1051, 1052, 1085, 1086, 1087, 1088, 1089, 1090, 1091, 1092, 1093, 1127, 1128, 1129, 1130, 1131, 1132, 1133, 1134, 1135, 1168, 1169, 1170, 1171, 1172, 1173, 1174, 1175, 1176, 1177, 1209, 1210, 1211, 1212, 1213, 1214, 1215, 1216, 1217, 1218, 1219, 1251, 1252, 1253, 1254, 1255, 1256, 1257, 1258, 1259, 1260, 1294, 1295, 1296, 1297, 1298, 1299, 1300, 1301, 1302, 1335, 1336, 1337, 1338, 1339, 1340, 1341, 1342, 1343, 1344, 1376, 1377, 1378, 1379, 1380, 1381, 1382, 1383, 1384, 1385, 1418, 1419, 1420, 1421, 1422, 1423, 1424, 1425, 1426, 1427, 1460, 1461, 1462, 1463, 1464, 1465, 1466, 1467, 1468, 1501, 1502, 1503, 1504, 1505, 1506, 1507, 1508, 1509, 1510, 1511, 1543, 1544, 1545, 1546, 1547, 1548, 1549, 1550, 1551, 1552, 1585, 1586, 1587, 1588, 1589, 1590, 1591, 1592, 1593, 1594, 1627, 1628, 1629, 1630, 1631, 1632, 1633, 1634, 1635, 1668, 1669, 1670, 1671, 1672, 1673, 1674, 1675, 1676, 1677, 1710, 1711, 1712, 1713, 1714, 1715, 1716, 1717, 1718, 1752, 1753, 1754, 1755, 1756, 1757, 1758, 1759, 1760, 1793, 1794, 1795, 1796, 1797, 1798, 1799, 1800, 1801, 1802, 1835, 1836, 1837, 1838, 1839, 1840, 1841, 1842, 1843, 1844, 1877, 1878, 1879, 1880, 1881, 1882, 1883, 1884, 1885, 1918, 1919, 1920, 1921, 1922, 1923, 1924, 1925, 1926, 1927, 1960, 1961, 1962, 1963, 1964, 1965, 1966, 1967, 1968, 1969};
        BitSet intersectBitset= new BitSet(2000);
        for(int element:intersecresult){
            intersectBitset.set(element);
        }
        BitSet mergeresult= bloomFilter.merge(other).getHashmap();
        Assert.assertTrue(intersectBitset.equals(mergeresult));
        Assert.assertTrue(bloomFilter.getElementsProcessed()==1670);
    }
}
