C**** mkTsMap.f:  f90 mkTsMap.f -o mkTsMap.exe
C**** This program reads monthly anomalies for 8000 subboxes,
C**** computes trends, anomalies,std.devns of a user-specified type
C**** (Jan,..,Winter,..,Annual,..) for a user-specified time
C**** period.
C****
C**** In this version we interpolate directly from the equal area grid
C**** to a regular latitude-longitude grid (90S->90N and 180W->180E)
C****
      INTEGER INFO(8),INFOO(8)
!     REAL  TIN(MONMX),  TAV(MONMX),   TOUT(8000,NOUT)
!     REAL TINO(MONMX), TAVO(MONMX),   TGCM(imo*jmo),WOUT(8000)
!     REAL  Tsum(NOUT),Tmin(NOUT),Tmax(NOUT)
      real, allocatable :: TIN(:),TAV(:),TINO(:),TAVO(:)
      real, allocatable :: TOUT(:,:),TGCM(:), Tsum(:),Tmin(:),Tmax(:)
      real                 WOUT(8000)
      CHARACTER*80 LINE,TITLE,TITLEO,TITOUT(4)
      CHARACTER*11 PER(20)
      CHARACTER*3 MNTH3(12)
      CHARACTER*5 tsk(4)
      character*20 TPOL(0:1)
      INTEGER IOFF(20),LSEASN(20), taskf,typef
!    *,M1(NOUT),M1b(NOUT),NAVG(NOUT),NAVGb(NOUT),
!    * LENAVG(NOUT),LMIN(NOUT),KM(NOUT),      MNYRSG(NOUT),MINY1G(NOUT),
!    * TASK(NOUT),TYPE(NOUT),IYR1(NOUT),IYR2(NOUT),IY1B(NOUT),IY2B(NOUT)
      integer,allocatable,dimension(:)::M1,M1b,Navg,Navgb,LenAvg,Lmin,km
      integer,allocatable,dimension(:) :: TASK,TYPE,MNYRSG,MINY1G
      integer,allocatable,dimension(:) :: IYR1,IYR2,  IY1B,  IY2B, kount
c     EQUIVALENCE (IY1b(1),MNYRSG(1))
c     EQUIVALENCE (IY2b(1),MINY1G(1))
      DATA TITOUT/
     *'_____________________________ L-OTI(^S^o^N^C) Anomaly vs 1951-80'
     *,'_____________________________ L-OTI(^S^o^N^C) Change xxxx-xxxx'
     *,'_____________________________ L-OTI(^S^o^N^C) Stand Deviations'
     *,'_____________________________ L-OTI(^S^o^N^C) Broken Trend'/
      DATA
     *  PER/'____January','___February','______March','______April',
     *      '________May','_______June','_______July','_____August',
     *      '__September','____October','___November','___December',
     *      'Dec-Jan-Feb','Mar-Apr-May','Jun-Jul-Aug','Sep-Oct-Nov',
     *      '_Annual_J-D','_Annual_D-N','_all_months','all_seasons'/,

     *  MNTH3/'Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep',
     *        'Oct','Nov','Dec'/,
     *  tsk/'Anoml','Trend','StDev','BrkTr'/,
     *  IOFF/0,1,2,3,4,5,6,7,8,9,10,11, -1,2,5,8, 0,-1,0,-1/,
     *  LSEASN/12*1, 4*3, 2*12, 1,3/,
     *  TPOL/'  no polar averaging',' use polar averaging'/
!!!   COMMON TITLE,TGCM
      logical :: series = .false.

C**** Open input and output files
      open(10,file='TS_DATA',form='unformatted',access='sequential')
      open( 9,file='SST_DATA',form='unformatted',access='sequential')
      open(11,file='out_data',form='unformatted',access='sequential')

C**** Initializations for equal-area grid
      CALL get_ij(0,0,1,ij)

C**** Read in first line of parameters
C****
      read(*,*) taskf,typef,IYR1f,IYR2f, IY1bf,IY2bf,
     *          rlnd,imo,jmo,offio,dlato
      iskip=0
      if(taskf.le.0) then
        iskip=abs(taskf)
        series=.true.
        taskf=1
        lastyr=iyr2f
        iyr2f=iyr1f
      end if
      ipol = 0                       ! no polar averaging in output grid
      if(dlato.lt.0.) then           ! force polar averaging
        dlato=-dlato
        ipol = 1
      end if
      write(*,*) 'gridding: 8000 subboxes->',imo,'x',jmo,tpol(ipol)
      dlpol = .5*( 180. - (jmo-2)*dlato )
      if(dlpol .le. 0.) then
        write(*,*) 'latitude belt width (degrees) is too big:',dlato
        write(*,*) 'the last parameter has to be < ',180./(jmo-2)
        stop
      else if ( dlato > 180./(jmo-1.5) .or. dlato < 180./(jmo+2) ) then
        write(*,*)
        write(*,*) '**************************************************'
        write(*,*) '*      WARNING : unusual grid dimensions:        *'
        write(*,*) '* latitude with of polar boxes:',dlpol,' degrees *'
        write(*,*) '* latitude with of other boxes:',dlato,' degrees *'
        write(*,*) '**************************************************'
        write(*,*)
      end if
C****
C**** Read, display, and use the header record
C****
      READ(10) INFO,TITLE
      MNOW=INFO(1)
      BAD=INFO(7)
      read(TITLE(33:36),*) ircrit
      rcrit=ircrit
      rland=int(rlnd)
      Rintrp=rlnd-rland
      if(rlnd.gt.rcrit) rland=bad
      if(rlnd.lt.0) rland=-bad
      write(6,*) 'rlnd,rland,Rintrp,bad',rlnd,rland,Rintrp,bad
      if(rland.eq.bad) write(6,*) 'OCEAN DATA ARE IGNORED'
      if(rlnd.lt.0.) write(6,*) 'LAND DATA ARE IGNORED'
      do 5 i=1,8
    5 INFOO(i)=INFO(i)
      if(rland.ne.bad) READ(9) INFOO,TITLEO
      MNOWO=INFOO(1)
c     WRITE(6,*) INFO
      WRITE(6,*) 'data type and source:'
      WRITE(6,*) TITLE
      if(rland.ne.bad) WRITE(6,*) TITLEO
      write(6,*) 'missing data flag:',INFO(7)
      KQ=INFO(2)
      MAVG=INFO(3)
      IF(MAVG.NE.6) STOP 'DATA ARE NOT MONTHLY MEANS'
      MONM=INFO(4)
      MONMO=INFOO(4)
      IYRBEG=INFO(6)
      IYRBGO=INFOO(6)
      IYRBGC=MIN(IYRBGO,IYRBEG)
      I1TIN=1+12*(IYRBEG-IYRBGC)
      I1TINO=1+12*(IYRBGO-IYRBGC)
      MONMC=MAX(MONM+I1TIN-1,MONMO+I1TINO-1)
      NOUT=max(12,MONMC) ! for series (task=0)
      IYREND=IYRBGC-1+MONMC/12

C**** Allocate all arrays
      allocate(TIN(MONMC),TINO(MONMC), TAV(MONMC),TAVO(MONMC))
      allocate(TOUT(8000,NOUT),Tsum(NOUT),Tmin(NOUT),Tmax(NOUT))
      allocate(M1(NOUT),M1b(NOUT),NAVG(NOUT),NAVGb(NOUT))
      allocate(LENAVG(NOUT),LMIN(NOUT),KM(NOUT),kount(NOUT))
      allocate(TASK(NOUT),TYPE(NOUT),IYR1(NOUT),IYR2(NOUT))
      allocate(IY1B(NOUT),IY2B(NOUT),MNYRSG(NOUT),MINY1G(NOUT))
      allocate(TGCM(imo*jmo))

C**** Initialize some arrays
      TOUT=BAD
      task(1)=taskf ; type(1)=typef ;   IYR1(1)=IYR1f ;   IYR2(1)=IYR2f
      IY1b(1)=IY1bf ; IY2b(1)=IY2bf
C****
C**** Get/report the user-specifiable parameters
C**** Read in sets of 6 parameters, find assoc. terms
C**** (first read - already done - required 4 extra parameters)
C****
      nmaps=1
      write(*,*)
     *       '                   Anoml type year1 year2  year1b year2b'
      write(*,*)
     *       '         OR:  Trend/StDv type year1 year2    %ok  okStrt'
      write(*,*)
     *       '         OR: BrokenTrend type year1 year2    %ok  yrBreak'
   30 write(*,'(A5,I5,A9,A5,I5,2I6,I8,I6)') '  MAP ',nmaps,' setting:',
     *   tsk(task(nmaps)),type(nmaps),IYR1(nmaps),IYR2(nmaps),
     *   IY1b(nmaps),IY2b(nmaps) !  same as: MNYRSG(nmaps),MINY1G(nmaps)
      MNYRSG(nmaps)=IY1b(nmaps) ; MINY1G(nmaps)=IY2b(nmaps)
C**** Modify the parameters if necessary, find related parameters
      typesv=TYPE(nmaps)
      ISEASN=TYPE(nmaps)
      if(ISEASN.eq.1203) ISEASN=13
      if(ISEASN.eq.0303) ISEASN=14
      if(ISEASN.eq.0603) ISEASN=15
      if(ISEASN.eq.0903) ISEASN=16
      if(ISEASN.eq.0112) ISEASN=17
      if(ISEASN.eq.1212) ISEASN=18
      if(ISEASN.gt.100) ISEASN=ISEASN/100
      if(ISEASN.gt.18.and.task(nmaps).eq.1) stop 'wrong task/type combo'
      moff=ioff(ISEASN)
      lenavg(nmaps)=lseasn(ISEASN)
      lsn=TYPE(nmaps)-100*ISEASN
      TYPE(nmaps)=ISEASN
      if(lsn.gt.0) then
         lenavg(nmaps)=lsn
         moff=ISEASN-1
         if(moff+lsn.gt.12) moff=moff-12
      end if                                                 ! PI6(.,3)
      IYOFF=0
      IF(MOFF.LT.0) IYOFF=-1
      IF(IYR1(nmaps)+IYOFF.LT.IYRBGC) STOP 'YEAR1 TOO LOW'
      if(IYR2(nmaps).lt.IYR1(nmaps)) IYR2(nmaps)=IYR1(nmaps)
      IF(IYR2(nmaps).GT.IYREND) STOP 'YEAR2 TOO HIGH'
      IF(TASK(nmaps).gt.1.AND.MNYRSG(nmaps).GT.100) MNYRSG(nmaps)=100
      IF(TASK(nmaps).gt.1.AND.MINY1G(nmaps).LT.IYR1(nmaps))
     *   MINY1G(nmaps)=IYR2(nmaps)+1
C****
C**** Set some more derived parameters
C****
C**** More than half the data have to be available to define a mean
      LMIN(nmaps)=LENAVG(nmaps)/2+1
C**** Change of base :         --- for Anomalies only
      moffb=moff
      if(task(nmaps).eq.1) then
        if(iy1b(nmaps).lt.1600) iy1b(nmaps)=1951
        if(iy1b(nmaps).gt.150000) then
           iy1bx=iy1b(nmaps)/100
           moffx=iy1b(nmaps)-iy1bx*100-1
           if(moffb.lt.moffx.and.lenavg(nmaps).eq.12) moffb=moffx
           if(moffb.lt.moffx) moffb=moffb+12
           iy1b(nmaps)=iy1bx
        end if
        if(iy2b(nmaps).lt.iy1b(nmaps)) iy2b(nmaps)=1980
      end if
C**** KM: Length of a full period
      KM(nmaps)=12
      IF(ISEASN.EQ.19) KM(nmaps)=1
      IF(ISEASN.EQ.20) KM(nmaps)=3
C**** M1,M1b: Location in TIN-array of 1st month needed
      M1(nmaps)=1+12*(IYR1(nmaps)-IYRBGC)+moff
      m1b(nmaps)=1+12*(iy1b(nmaps)-IYRBGC)+moffb
C**** NAVG,NAVGb: Length of time series of means of the selected type
      NAVG(nmaps)=(IYR2(nmaps)+1-IYR1(nmaps))*12/KM(nmaps)
      navgb(nmaps)=iy2b(nmaps)+1-iy1b(nmaps)
C**** MINY1G-->Position in final series of upper limit for 1st good data
      IF(TASK(nmaps).gt.1)      ! or last year before break in slope
     *   MINY1G(nmaps)=(MINY1G(nmaps)+1-IYR1(nmaps))*12/KM(nmaps)
      if(TASK(nmaps).eq.4) then
         if(nmaps.eq.NOUT) then  ! can't satisfy the request
           nmaps=NOUT-1
           go to 80
         end if
         TASK(nmaps+1)=0
         nmaps=nmaps+1
      end if
      if(nmaps.eq.NOUT) go to 80
      if(series) then
        task(nmaps+1)=1
        IY1b(nmaps+1)=IY1b(nmaps)
        IY2b(nmaps+1)=IY2b(nmaps)
        if(lenavg(nmaps).eq.12.or.iskip.gt.0) then        ! ann. means
           type(nmaps+1)=typesv
           IYR1(nmaps+1)=IYR1(nmaps)+max(1,iskip)
        else if (iseasn.ge.13 .and. iseasn.le.16) then ! seas.means
           type(nmaps+1)=type(nmaps)+1
           IYR1(nmaps+1)=IYR1(nmaps)
           if(type(nmaps+1).eq.17) then
             type(nmaps+1)=13
             IYR1(nmaps+1)=IYR1(nmaps)+1
           end if
        else                      ! N-month means N=lenavg(nmaps)=1->11
           ISEASN=ISEASN+lenavg(nmaps)
           IYR1(nmaps+1)=IYR1(nmaps)
           if(ISEASN.gt.12) ISEASN=ISEASN-12
           if(ISEASN+lenavg(nmaps).gt.13 .or. ISEASN.eq.1)
     *     IYR1(nmaps+1)=IYR1(nmaps)+1
           type(nmaps+1)=ISEASN*100+lenavg(nmaps)
        end if
        IYR2(nmaps+1)=IYR1(nmaps+1)
        if(IYR2(nmaps+1).gt.lastyr) go to 80
      else
        read(*,*,END=80) task(nmaps+1),type(nmaps+1),
     *   IYR1(nmaps+1),IYR2(nmaps+1), IY1b(nmaps+1),IY2b(nmaps+1)
      end if
      nmaps=nmaps+1
      go to 30
C****
C**** Loop over subboxes - find output data, their range and mean
C****
   80 CONTINUE

      if(nmaps.eq.1) then
C****   Open more output files
        open(21,file='zonal.txt',form='formatted')
        open(31,file='grid.txt',form='formatted')
      end if

      DO 90 k=1,nmaps
      TMIN(k)=1.E20
      TMAX(k)=-TMIN(k)
      TSUM(k)=0.
   90 KOUNT(k)=0
      DO 100 N=1,8000
      IF(MOD(N,1000).EQ.0) WRITE(6,*)8000-N,' records to go'
C**** Read in time series of monthly means
      DO 95 M=1,MONMC
      TIN(M)=BAD
   95 TINO(M)=BAD
      CALL SREAD (10,TIN(I1TIN),MNOW,LATS,LATN,LONW,LONE,   DL,MXN)
      MNOW=MXN
      if(rland.lt.bad) then
        CALL SREAD (9,TINO(I1TINO),MNOWO, LTS1,LTN1,LNW1,LNE1, DLO,MXON)
        if(lats>7500) TINO=bad !! hack to extend ocean ice mask
        MNOWO=MXON
        wocn=max(0.,(DL-rland)/(rcrit-rland+1.e-10))
        if(wocn.gt.Rintrp) wocn=1.
      end if
      do 100 k=1,nmaps
      if(TASK(k).eq.0) go to 100
C**** Collect the desired means for years IYR1->IYR2 in TAV-array
      tavb=0.
      tavbO=0.
      TAVO(1)=BAD
      if(task(k).eq.1) then
        CALL AVG(TIN(M1b(k)),KM(k),NAVGb(k),LENAVG(k),BAD,LMIN(k),TAV)
        if(NAVGb(k).gt.1) CALL AVG(TAV,1,1,NAVGb(k),BAD,1,TAV)
        if(rland.lt.bad) then
C---  CALL AVG(TINO(M1b(k)),KM(k),NAVGb(k),LENAVG(k),BAD,LENAVG(k),TAVO)
      CALL AVG(TINO(M1b(k)),KM(k),NAVGb(k),LENAVG(k),BAD,LMIN(k),TAVO)
        if(NAVGb(k).gt.1) CALL AVG(TAVO,1,1,NAVGb(k),BAD,1,TAVO)
        end if
        tavb=TAV(1)
        tavbO=TAVO(1)
      end if
      TAV(1)=BAD
      TAVO(1)=BAD
      if(tavb.ne.BAD)
     *  CALL AVG(TIN(M1(k)),KM(k),NAVG(k),LENAVG(k),BAD,LMIN(k),TAV)
      if(rland.lt.bad.and.tavbO.ne.BAD)
C--- *  CALL AVG(TINO(M1(k)),KM(k),NAVG(k),LENAVG(k),BAD,LENAVG(k),TAVO)
     *  CALL AVG(TINO(M1(k)),KM(k),NAVG(k),LENAVG(k),BAD,LMIN(k),TAVO)
C**** Replace TAV(1) by the desired mean or trend
      IF(TASK(k).EQ.1.AND.NAVG(k).GT.1) then
          if(tavb.ne.BAD) CALL AVG(TAV,1,1,NAVG(k),BAD,1,TAV)
          if(rland.lt.bad.and.tavbO.ne.BAD)
     *                    CALL AVG(TAVO,1,1,NAVG(k),BAD,1,TAVO)
      END IF
      MN1=MAX(2,(NAVG(k)*MNYRSG(k)+99)/100)
      IF(TASK(k).EQ.2) THEN
          CALL TREND(TAV,NAVG(k),BAD,MN1,MINY1G(k))
          if(rland.lt.bad)
     *       CALL TREND(TAVO,NAVG(k),BAD,MN1,MINY1G(k))
      END IF
      IF(TASK(k).EQ.3) THEN
          CALL STDV(TAV,NAVG(k),BAD,MN1)
          if(rland.lt.bad)
     *       CALL STDV(TAVO,NAVG(k),BAD,MN1)
      END IF
      IF(TASK(k).EQ.4) then
         Xmid=MINY1G(k)+.5
         MN1=MAX(2,(MINY1G(k)*MNYRSG(k)+99)/100)
         MN2=MAX(2,((NAVG(k)-MINY1G(k))*MNYRSG(k)+99)/100)
         CALL TREND2(TAV,NAVG(k),Xmid,BAD,MN1,MN2,   sl1 ,  sl2)
         CALL TREND2(TAVO,NAVG(k),Xmid,BAD,MN1,MN2,   sl1o,  sl2o)
         if(sl1.eq.bad) then
            sl1=sl1o
            sl2=sl2o
         end if
         if(sl1o.ne.bad) then
            sl1=sl1*(1.-wocn)+sl1o*wocn
            sl2=sl2*(1.-wocn)+sl2o*wocn
         end if
         TAV(1)=sl1
         if(sl2.ne.BAD) then
           CALL get_ij((LATS+LATN)/2,(LONW+LONE)/2,0,ij)
           TOUT(ij,k+1)=sl2
           IF(sl2.GT.TMAX(k+1)) TMAX(k+1)=sl2
           IF(sl2.LT.TMIN(k+1)) TMIN(k+1)=sl2
           TSUM(k+1)=TSUM(k+1)+sl2
           KOUNT(k+1)=KOUNT(k+1)+1
        end if
      else
        if(TAV(1).eq.BAD.or.rlnd.lt.0.) then
           tavb=tavbO
           TAV(1)=TAVO(1)
        end if
        if(TAVO(1).ne.BAD) then
           tavb=tavb*(1.-wocn)+tavbO*wocn
           TAV(1)=TAV(1)*(1.-wocn)+TAVO(1)*wocn
        end if
      end if
      if(TAV(1).EQ.BAD) go to 100
      TAV(1)=TAV(1)-tavb
C**** Put TAV(1) at the appropriate places in the output array
      CALL get_ij((LATS+LATN)/2,(LONW+LONE)/2,0,ij)
      TOUT(ij,k)=TAV(1)
      IF(TAV(1).GT.TMAX(k)) TMAX(k)=TAV(1)
      IF(TAV(1).LT.TMIN(k)) TMIN(k)=TAV(1)
      TSUM(k)=TSUM(k)+TAV(1)
      KOUNT(k)=KOUNT(k)+1
  100 CONTINUE
C**** End of loop over subboxes - Initiate interpolation to model grid
      call eantrp0 (imo,jmo,offio,180./dlato,BAD)
C****
C**** Report title, mean and range
C****
      DO 200 k=1,nmaps
      if (TASK(k).eq.0) then
         WRITE(TITLE(13:16),'(I4)') IY2b(k-1)+IYR1(k-1)
         WRITE(TITLE(18:23),'(A2,I4)') '->',IYR2(k-1)
         go to 190
      end if
      TITLE=TITOUT(TASK(k))
      IF(rlnd.lt.0.) TITLE(31:35)=' Tocn'
      IF(rland.eq.bad) TITLE(31:35)='Tsurf'
      TITLE(1:11)=PER(TYPE(k))
      if(TYPE(k).lt.13.and.lenavg(k).gt.1) then
         iseas2=TYPE(k)+lenavg(k)-1
         if(iseas2.gt.12) iseas2=iseas2-12
         TITLE(1:11)='    '//MNTH3(TYPE(k))//'-'//MNTH3(iseas2)
      end if
      if(TASK(k).NE.2) WRITE(TITLE(13:16),'(I4)') IYR1(k)
      if(TASK(k).EQ.2)
     *  WRITE(TITLE(54:62),'(I4,A1,I4)') IYR1(k),'-',IYR2(k)
      IF(IYR2(k).GT.IYR1(k)) THEN
        IF(TASK(k).EQ.1) WRITE(TITLE(17:21),'(A1,I4)')  '-',IYR2(k)
        IF(TASK(k).GT.2) WRITE(TITLE(17:22),'(A2,I4)') '->',IYR2(k)
        IF(TASK(k).EQ.4) WRITE(TITLE(19:22),'(I4)') IYR1(k)+IY2b(k)-1
      END IF
      if(task(k).eq.1)
     *    WRITE(TITLE(58:66),'(I4,A1,I4)') Iy1b(k),'-',Iy2b(k)
  190 TAVG=BAD
      if(series.and.kount(k).eq.0) stop
      IF(KOUNT(k).GT.0) TAVG=TSUM(k)/KOUNT(k)
      WRITE(6,*) TITLE
C     if(k.lt.4) write(99,*) TITLE(26:80)
      WRITE(6,*) 'Range,Mean',TMIN(k),TMAX(k),TAVG
C****
C**** Interpolate to model grid and save to disk
C****
      do i=1,8000
        WOUT(i)=1.
        if(TOUT(i,k).eq.BAD) WOUT(i)=0.
      end do
      if(ipol.eq.1) call eantrpp (WOUT,TOUT(1,k),TGCM)
      if(ipol.eq.0) call eantrp  (WOUT,TOUT(1,k),TGCM)
  200 CALL SWRITE(11,TITLE,TGCM,imo,jmo,bad,nmaps,180./dlato,offio)
      STOP
      END

      SUBROUTINE SWRITE(NDISK,TITLE,ARRAY,IM,JM,bad,nmaps,divj,offi)
      REAL ARRAY(IM,JM)
      character*80 TITLE
      WRITE(NDISK) TITLE,ARRAY
      if(nmaps.ne.1) return
C**** Zonal Means
      write(ndisk+20,'(a)') title
      write(ndisk+20,'(a)') '   i   j     lon     lat    array(i,j)'
      write(ndisk+10,'(a)') title
      write(ndisk+10,'(a)') 'latitude'
      write(ndisk+10,'(a)') 'Zonal Mean'
      write(ndisk+10,'(a)') 'lat  skip'
      dj=180./divj
      offj=.5*(divj-jm)
      do j=1,jm
      xlat=-90.+dj*(j-.5+offj)
      if(xlat.lt.-90.) xlat=-90.
      if(xlat.gt. 90.) xlat= 90.
      zavs=0.
      kount=0
      do i=1,im
      xlon=-180.+(360./im)*(i-.5+offi)
      write(ndisk+20,'(2i4,2f8.2,f14.4)') i,j,xlon,xlat,array(i,j)
      if(array(i,j).ne.bad) then
        kount=kount+1
        zavs=zavs+array(i,j)
      end if
      end do
      if(kount.gt.0) then
        write(ndisk+10,*) xlat,zavs/kount
      else
        write(ndisk+10,*) xlat,' * '
      end if
      end do
      RETURN
      END SUBROUTINE SWRITE

      SUBROUTINE SREAD (NDISK,ARRAY,NDIM, N1,N2,N3,N4, DSTN,MXN)
      REAL ARRAY(NDIM)
      READ(NDISK) MXN, N1,N2,N3,N4, NR1,NR2, DSTN, ARRAY
      RETURN
      END SUBROUTINE SREAD

      SUBROUTINE AVG(ARRAY,KM,NAVG,LAV,BAD,LMIN, DAV)
      REAL ARRAY(KM,NAVG),DAV(NAVG)
      DO 100 N=1,NAVG
      SUM=0.
      KOUNT=0
      DO 50 L=1,LAV
      IF(ARRAY(L,N).EQ.BAD) GO TO 50
      SUM=SUM+ARRAY(L,N)
      KOUNT=KOUNT+1
   50 CONTINUE
      DAV(N)=BAD
      IF(KOUNT.GE.LMIN) DAV(N)=SUM/KOUNT
  100 CONTINUE
      RETURN
      END SUBROUTINE AVG

      SUBROUTINE TREND(A,LEN,BAD,MINOKS,MIN1OK)
C**** finds a linear fit using regression analysis
C**** A(1-->LEN) - TIME SERIES TO BE FITTED.
C**** A result is reported (as A(1)) only if
C**** 1) the series has at least MINOKS good data (MINOKS > 1 !)
C**** 2) A(N) has to be good for at least one N < MIN1OK+1
      REAL*8 SUMA,SUMAN,AX
      REAL A(*)
      NFIRST=0
      KOUNT=0
      NSUM=0
      NNSUM=0.
      SUMA=0.
      SUMAN=0.
      DO 10 N=LEN,1,-1
      IF(A(N).EQ.BAD) GO TO 10
      NFIRST=N
      KOUNT=KOUNT+1
      NSUM=NSUM+N
      NNSUM=NNSUM+N**2
      SUMA=SUMA+A(N)
      SUMAN=SUMAN+A(N)*N
   10 CONTINUE
C**** Find trend (= slope*(LEN-1)) and set A(1)=trend
      A(1)=BAD
      IF(KOUNT.LT.MINOKS) RETURN
      IF(NFIRST.GT.MIN1OK) RETURN
      AX=KOUNT*NNSUM-NSUM**2
      A(1)=(LEN-1)*(SUMAN*KOUNT-SUMA*NSUM)/AX
      RETURN
      END SUBROUTINE TREND

      SUBROUTINE STDV(A,LEN,BAD,MINOKS)
C**** finds the Standard Deviation of the given time series
C**** A(1-->LEN) - TIME SERIES .
C**** A result is reported (as A(1)) only if
C**** the series has at least MINOKS good data (MINOKS > 1 !)
      REAL*8 SUMA,SUMA2
      REAL A(*)
      KOUNT=0
      SUMA=0.
      SUMA2=0.
      DO 10 N=1,LEN
      IF(A(N).EQ.BAD) GO TO 10
      KOUNT=KOUNT+1
      SUMA=SUMA+A(N)
      SUMA2=SUMA2+A(N)*A(N)
   10 CONTINUE
C**** Find standard deviation and set A(1)=std.dev
      A(1)=BAD
      IF(KOUNT.LT.MINOKS) RETURN
      A(1)=SQRT( (SUMA2-SUMA**2/KOUNT)/(KOUNT-1) )
      RETURN
      END SUBROUTINE STDV

      SUBROUTINE TREND2(A,LEN,Xmid,BAD,LIM1,LIM2, SL1,SL2)
C**** finds a fit using regression analysis by a line
C**** with a break in slope at Xmid. Returned are the 2 slopes
C**** SL1,SL2 provided we have at least LIM1,LIM2 data.
      REAL A(*)
      REAL*8 sx(2),sxx(2),sxa(2),sa,denom,xnum1,xnum2
      INTEGER kount(2)

      sl1=bad
      sl2=bad
c     Ymid=bad
      sa=0
      do k=1,2
        kount(k)=0
        sx(k)=0.
        sxx(k)=0.
        sxa(k)=0.
      end do

      do 100 n=1,len
      if(a(n).eq.BAD) go to 100
      x=n-Xmid
      sa=sa+a(n)
      k=1
      if(x.gt.0.) k=2
      kount(k)=kount(k)+1
      sx(k)=sx(k)+x
      sxx(k)=sxx(k)+x**2
      sxa(k)=sxa(k)+x*a(n)
  100 continue

      ntot=kount(1)+kount(2)
      denom=ntot*sxx(1)*sxx(2)-sxx(1)*sx(2)**2-sxx(2)*sx(1)**2
      xnum1=sx(1)*(sx(2)*sxa(2)-sxx(2)*sa)+sxa(1)*(ntot*sxx(2)-sx(2)**2)
      xnum2=sx(2)*(sx(1)*sxa(1)-sxx(1)*sa)+sxa(2)*(ntot*sxx(1)-sx(1)**2)

      if(kount(1).lt.LIM1.or.kount(2).lt.LIM2) return
cw        write(88,*) 'len,kts,denom,num1,num2',
cw   *      len,kount,denom,xnum1,xnum2
cw        if(denom.eq.0.) then
cw        write(88,*) 'sx,sxx',sx,sxx
cw        write(88,*) 'sxa,sa',sxa,sa
cw        stop '0-denom'
cw       end if
      sl1=xnum1/denom
      sl2=xnum2/denom
c     Ymid=(sa-sl1*sx(1)-sl2*sx(2))/ntot

      return
      end SUBROUTINE TREND2

      SUBROUTINE EANTRP0 (INB,JNB,OFFIB,DIVJB, SKIB)
C**** modified by R.Ruedy from a program written by Dr. Gary L. Russell
C****                                                      at NASA/GISS
C**** EANTRP performs a horizontal interpolation of per unit area or per
C**** unit mass quantities defined on grid A, calculating the quantity
C**** on grid B.  B grid values that cannot be calculated because the
C**** covering A grid boxes have WTA = 0, are set to the value of SKIP.
C**** The area weighted integral of the quantity is conserved.
C****
      IMPLICIT REAL*8 (A-H,O-Z)
      PARAMETER (TWOPI=6.283185307179586477d0)
C     REAL*4  WTA(INA,JNA),A(INA,JNA),B(INB,JNB),
      REAL*4  WTA(8000),      A(8000),      B(*),
     *        OFFIA,DIVJA, OFFIB,DIVJB, SKIB,SKIP
      real*8  SINA(0:80),SINB(0:361),ibefor(81),
     *        FMIN(720,4),FMAX(720,4),GMIN(361),GMAX(361)
      integer IMIN(720,4),IMAX(720,4),JMIN(362),JMAX(362)
      save SINA,SINB,ibefor,FMIN,FMAX,GMIN,GMAX,IMIN,IMAX,JMIN,JMAX
      LOGICAL*4 QMPOLE
      DATA IMB,JMB/2*0/, SKIP/0/
C****
C     IMA = 40*izone(iband) izone=1,2,3,4,4,3,2,1
      JMA = 80
      IMB = INB
      JMB = JNB
      SKIP = SKIB

      IF(IMB.lt.1 .or. IMB.gt.720 .or. JMB.lt.1 .or. JMB.gt.361)
     *   GO TO 400
C****
C**** Partitions in the I direction
C**** RIA = longitude in degrees of right edge of grid box IA on grid A
C**** RIB = longitude in degrees of right edge of grid box IB of grid B
C**** IMIN(IB) = box on grid A containing left edge of box IB on B
C**** IMAX(IB) = box on grid A containing right edge of box IB on B
C**** FMIN(IB) = fraction of box IMIN(IB) on A that is left of box IB
C**** FMAX(IB) = fraction of box IMAX(IB) on A that is right of box IB
C****
      do izone=1,4
      DIA = 360d0/(izone*40)
      DIB = 360d0/IMB
      IA  = 1
      RIA = IA*DIA - 360
      IB  = IMB
      DO 150 IBP1=1,IMB
      RIB = (IBP1-1+OFFIB)*DIB
  110 IF(RIA-RIB)  120,130,140
  120 IA  = IA  + 1
      RIA = RIA + DIA
      GO TO 110
C**** Right edge of A box IA and right edge of B box IB coincide
  130 IMAX(IB,izone) = IA
      FMAX(IB,izone) = 0.
      IA  = IA  + 1
      RIA = RIA + DIA
      IMIN(IBP1,izone) = IA
      FMIN(IBP1,izone) = 0.
      GO TO 150
C**** A box IA contains right edge of B box IB
  140 IMAX(IB,izone) = IA
      FMAX(IB,izone) = (RIA-RIB)/DIA
      IMIN(IBP1,izone) = IA
      FMIN(IBP1,izone) = 1.-FMAX(IB,izone)
  150 IB = IBP1
      IMAX(IMB,izone) = IMAX(IMB,izone) + 40*izone
!       WRITE (0,*) 'Zone=',izone,' IMA=',40*izone
!       WRITE (0,*) 'IMIN=',(IMIN(I,izone),I=1,IMB)
!       WRITE (0,*) 'IMAX=',(IMAX(I,izone),I=1,IMB)
!       WRITE (0,*) 'FMIN=',(FMIN(I,izone),I=1,IMB)
!       WRITE (0,*) 'FMAX=',(FMAX(I,izone),I=1,IMB)
      end do
C****
C**** Partitions in the J direction
C****
C**** RJA = latitude in radians at top edge of box JA on grid A
C**** SINA(JA) = sine of latitude of top edge of box JA on grid A
      SINA(0)  = -1. ; sband = SINA(0) ! sine of northern edge of band
      do izone=1,4
        dsband = .1d0 * izone
        do jzs=1,10
          ja = jzs + (izone-1)*10
          SINA(JA) = sband + .1d0*jzs*dsband
        end do
        sband = sband + dsband
      end do
      SINA(40)  = 0.
      do ja=41,80
        sina(ja) = -sina(80-ja)
      end do
      ja=1
      ibefor(1) = 0
      do izone=1,8
      iz = izone ; if(iz>4) iz = 9-izone
      do jzs=1,10
      ibefor(ja+1) = ibefor(ja) + 40*iz
      ja=ja+1
      end do
      end do
!       WRITE (0,*) 'JA, sina(ja)           ',0,sina(0)
!       do ja=1,80
!         WRITE (0,*) 'JA, sina(ja), ibef(ja)=',ja,sina(ja),ibefor(ja)
!       end do
!       WRITE (0,*) 'JA,           ibef(ja)=',81,ibefor(81)
C**** RJB = latitude in radians at top edge of box JB on grid B
C**** SINB(JB) = sine of latitude of top edge of box JB on grid B
      OFFJB = (DIVJB-JMB)/2.
      DJB   = .5*TWOPI/DIVJB
      DO 220 JB=1,JMB-1
      RJB = (JB+OFFJB)*DJB - .25*TWOPI
  220 SINB(JB) = DSIN(RJB)
      SINB(0)  = -1.
      SINB(JMB)=  1.
C****
C**** JMIN(JB) = index of box of A that contains bottom edge of box JB
C**** JMAX(JB) = index of box of A that contains top edge of box JB
C**** GMIN(JB) = fraction of box JMIN(JB) on A grid that is below box JB
C**** GMAX(JB) = fraction of box JMAX(JB) on A grid that is above box JB
C****
      JMIN(1) = 1
      GMIN(1) = 0.
      JA = 1
      DO 350 JB=1,JMB-1
  310 IF(SINA(JA)-SINB(JB))  320,330,340
  320 JA = JA + 1
      GO TO 310
C**** Top edge of A box JA and top edge of B box JB coincide
  330 JMAX(JB) = JA
      GMAX(JB) = 0.
      JA = JA + 1
      JMIN(JB+1) = JA
      GMIN(JB+1) = 0.
      GO TO 350
C**** A box JA contains top edge of B box JB
  340 JMAX(JB) = JA
      GMAX(JB) = SINA(JA)-SINB(JB)
      JMIN(JB+1) = JA
      GMIN(JB+1) = SINB(JB)-SINA(JA-1)
  350 CONTINUE
      JMAX(JMB) = JMA
      GMAX(JMB) = 0.
!       WRITE (0,*) 'JMIN=',(JMIN(J),J=1,JMB)
!       WRITE (0,*) 'JMAX=',(JMAX(J),J=1,JMB)
!       WRITE (0,*) 'GMIN=',(GMIN(J),J=1,JMB)
!       WRITE (0,*) 'GMAX=',(GMAX(J),J=1,JMB)
!       WRITE (0,*) 'Ibef=',(Ibefor(J),J=1,JMB)
      RETURN
C****
C**** Invalid parameters or dimensions out of range
C****
  400 WRITE (0,940) IMB,JMB,OFFIB,DIVJB, SKIP
      STOP 400
  940 FORMAT ('0Arguments received by EANTRP0 in order:'/
     *   2I12,' = IMB,JMB = array dimensions for B grid'/
     *  E24.8,' = OFFIB   = fractional number of grid boxes from',
     *                    ' IDL to left edge of grid box I=1'/
     *  E24.8,' = DIVJB   = number of whole grid boxes from SP to NP'/
     *  E24.8,' = SKIP    = value to be put in B array when B',
     *  ' grid box is subset of A grid boxes with WTA = 0'/
     *  '0These arguments are invalid or out of range.')
C****

      ENTRY EANTRP (WTA,A,B)
C****
C**** EANTRP performs the horizontal interpolation
C**** Input: WTA = weighting array for values on the A grid
C****          A = per unit area or per unit mass quantity
C**** Output:  B = horizontally interpolated quantity on B grid
C****
      QMPOLE = .FALSE.
      GO TO 500

      ENTRY EANTRPP (WTA,A,B)
C****
C**** EANTRPP is similar to EANTRP but polar values are replaced by
C**** their longitudinal mean
C****
      QMPOLE = .TRUE.
C****
C**** Interpolate the A grid onto the B grid
C****
  500 DO 520 JB=1,JMB
      JAMIN = JMIN(JB)
      JAMAX = JMAX(JB)
      DO 520 IB=1,IMB
      IJB  = IB + IMB*(JB-1)
      WEIGHT= 0.
      VALUE = 0.
      DO 510 JA=JAMIN,JAMAX
      izone = (ja + 9)/10
      if(izone > 4) izone=9-izone
      ima = izone*40
      IAMIN = IMIN(IB,izone)
      IAMAX = IMAX(IB,izone)
      G = SINA(JA)-SINA(JA-1)
      IF(JA.eq.JAMIN)  G = G - GMIN(JB)
      IF(JA.eq.JAMAX)  G = G - GMAX(JB)
      DO 510 IAREV=IAMIN,IAMAX
      IA  = 1+MOD(IAREV-1,IMA)
      IJA = IA + Ibefor(JA)
      F   = 1.
      IF(IAREV.eq.IAMIN)  F = F - FMIN(IB,izone)
      IF(IAREV.eq.IAMAX)  F = F - FMAX(IB,izone)
      WEIGHT = WEIGHT + F*G*WTA(IJA)
  510 VALUE  = VALUE  + F*G*WTA(IJA)*A(IJA)
      B(IJB) = SKIP
      IF(WEIGHT.ne.0.)  B(IJB) = VALUE/WEIGHT
  520 continue
C****
C**** Replace individual values near the poles by longitudinal mean
C****
      IF(.NOT.QMPOLE)  RETURN
      DO 630 JB=1,JMB,JMB-1
      WEIGHT = 0.
      VALUE  = 0.
      DO 610 IB=1,IMB
      IJB  = IB + IMB*(JB-1)
      IF(B(IJB).eq.SKIP)  GO TO 610
      WEIGHT = WEIGHT + 1.
      VALUE  = VALUE  + B(IJB)
  610 continue
      BMEAN = SKIP
      IF(WEIGHT.ne.0.)  BMEAN = VALUE/WEIGHT
      DO 620 IB=1,IMB
      IJB  = IB + IMB*(JB-1)
  620 B(IJB) = BMEAN
  630 continue
      RETURN
      END SUBROUTINE EANTRP0

      subroutine get_ij(lat,lon,ifirst,ij)
! Given the latitude and longitude of the center, get_ij finds the
! position of the grid box if they are arranged in the order:
!     latitudes 90S to 90N, and 180W to 180E within each latitude

      integer,save :: lats(81),nbefor(81)

      if(ifirst==1) then
! Set LATS(j),   the j-th Southern latitude edge in .01 degrees
! and IBEFOR(j), the number of grid boxes South of LATS(j)
        xbypi = 9000d0/asin(1d0)        ! 100 * 180/pi
        j=1 ; nbefor(1)=0 ; sband = -1. ! sine of southern edge of band
        do iband=1,8
          iz = iband ; if(iz>4) iz = 9-iband
          dsband = .1d0 * iz
          do jzs=1,10
            nbefor(j+1) = nbefor(j) + 40*iz
            LATS(J) = nint(xbypi*asin(sband + .1d0*(jzs-1)*dsband))
            j = j + 1
          end do
          sband = sband + dsband
        end do
        LATS(81)  = -lats(1)   ! 9000
C          do j=1,81
C            WRITE (0,*) 'J,lats,nbefor=',j,lats(j),nbefor(j)
C          end do
        return
      end if

c**** find "j" 1 -> 80 using a binary search
      jl = 1 ; jh = 80
   10 jm = (jl+jh)/2
      if(lat<lats(jm)) then
        jh = jm-1
      else
        jl = jm
      end if
      if(lat>lats(jl+1).and.lat<lats(jh)) go to 10
      j=jl
      if(lat>lats(jh)) j=jh

c**** find zone and "i"
      izon = (j+9)/10          ! 1 2 3 4 4 3 2 1
      if(izon>4) izon = 9-izon
      idlon = 900/izon         ! 100 * 360/(40*izon)
      i = 1 + ( lon + 18000 )/idlon

      ij = i + nbefor(j)

      return
      end subroutine get_ij
