libname win32 'data'
    outrep=WINDOWS_32;

libname win64 'data'
    outrep=WINDOWS_64;

libname lin32 'data'
    outrep=LINUX_32;

libname lin64 'data'
    outrep=LINUX_X86_64;

libname aix32 'data'
    outrep=RS_6000_AIX_32;

libname aix64 'data'
    outrep=RS_6000_AIX_64;

proc import
    datafile='data/test1.csv'
    dbms='csv'
    out=test0;

data test1;
    set test0;
    format Column4 mmddyy10.;
    format Column12 mmddyy10.;

data win32.test1(compress=no);
    set test1;

data win32.test2(compress=char);
    set test1;

data win32.test3(compress=binary);
    set test1;

data win64.test4(compress=no);
    set test1;

data win64.test5(compress=char);
    set test1;

data win64.test6(compress=binary);
    set test1;

data lin64.test7(compress=no);
    set test1;

data lin64.test8(compress=binary);
    set test1;

data lin64.test9(compress=char);
    set test1;

data aix32.test10(compress=no);
    set test1;

data aix32.test11(compress=binary);
    set test1;

data aix32.test12(compress=char);
    set test1;

data aix64.test13(compress=no);
    set test1;

data aix64.test14(compress=binary);
    set test1;

data aix64.test15(compress=char);
    set test1;

run;
