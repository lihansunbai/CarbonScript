C  FORMAT NUMBER FOR DATA INPUT
          NIN=12
C   FORMAT NUMBER FOR DATA OUTPUT
          NOUT=6
C FORMAT HEADINGS FOR OUTPUT LISTING
          WRITE(NOUT,100)
100       FORMAT(1X,
     1           'Table 1. Production of CO2 from Fossil Fuels and Ce
     1ment, 1860-1981.'/
     2'                       (Units = 10**6 tons of carbon)'/)
1         CONTINUE
          WRITE(NOUT,101)
101       FORMAT(67(1H-)/10X,'SOLID',2X,'LIQUID',1X,'NATURAL',3X,
     1'GAS',4X,'CEMENT',8X,'CUMULATIVE'/3X,'YEAR',3X,'FUELS',3X,'FUELS',
     23X,'GAS',3X,'FLARING',2X,'MANUF.',2X,'TOTAL',3X,'TOTALS'/
     367(1H-)/)
C PRINT 40 VALUES PER PAGE. READ DATA FROM TAPE.
          DO 50 I=1,40
          READ(NIN,150,END=99)IYR,SOLID,LIQUID,NATURL,GASFL,CEMENT,
     1TOTAL,CUMUL
150       FORMAT(I4,2X,6F7.1,1X,F10.1)
C WRITE DATA VALUES OUT.
          WRITE(NOUT,160) IYR,SOLID,LIQUID,NATURL,GASFL,CEMENT,
     1TOTAL,CUMUL
160       FORMAT(3X,I4,1X,F7.1,1X,F7.1,F7.1,1X,F7.1,1X,F7.1,2X,
     1  F7.1,F10.1)
50        CONTINUE
200       WRITE(NOUT,170)
170       FORMAT(1H1/)
          GO TO 1
99        CONTINUE
C WRITE OUT FOOTNOTE.
          WRITE(NOUT,105)
105       FORMAT(67(1H-)/3X,
     1'na=not available; applies to gas flaring and cement'/'manufactur
     1ing prior to 1950.')
          STOP
          END
