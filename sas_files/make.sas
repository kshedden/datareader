libname sasfiles '.';

data test1;
    infile 'test1.csv' delimiter=',';
    input var0 var1 : $10. var2 var3 : yymmdd8. var4 var5 : $10. var6 var7 : yymmdd8. var8 var9 : $10.;
    format var3 mmddyy10.;
    format var7 mmddyy10.;

data sasfiles.test1_compression_no(compress=no);
    set test1;

data sasfiles.test1_compression_char(compress=char);
    set test1;

data sasfiles.test1_compression_binary(compress=binary);
   set test1;

run;
