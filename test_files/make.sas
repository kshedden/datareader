libname sasfiles 'data';

proc import
    datafile='data/test1.csv'
    dbms='csv'
    out=test0;

data test1;
    set test0;
    format Column4 mmddyy10.;
    format Column12 mmddyy10.;

data sasfiles.test1_compression_no(compress=no);
    set test1;

data sasfiles.test1_compression_char(compress=char);
    set test1;

data sasfiles.test1_compression_binary(compress=binary);
   set test1;


run;
