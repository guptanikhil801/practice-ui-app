import { Injectable, OnDestroy } from '@angular/core';
import {
  Observable, Subject, BehaviorSubject, ReplaySubject,
  of, from, interval, timer, combineLatest, forkJoin,
  merge, concat, zip, throwError, race, iif,
} from 'rxjs';
import {
  map, filter, tap, take, takeUntil, takeWhile, takeLast,
  skip, skipWhile, skipUntil,
  switchMap, mergeMap, concatMap, exhaustMap,
  debounceTime, throttleTime, auditTime, sampleTime,
  distinctUntilChanged, catchError, retry, finalize,
  withLatestFrom, startWith, scan, reduce, bufferTime,
  shareReplay, share, delay, timeout, first, last, find,
  pairwise, toArray, count, groupBy, partition,
} from 'rxjs/operators';

// ─────────────────────────────────────────────────────────────────
// Usage in any component:
//   private rxjs = inject(RxjsOperatorsService);
//   ngOnInit() { this.rxjs.demoMap(); }
// ─────────────────────────────────────────────────────────────────

@Injectable({ providedIn: 'root' })
export class RxjsOperatorsService implements OnDestroy {

  private destroy$ = new Subject<void>();

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  // ─────────────────────────────────────────────
  // 1. TRANSFORMATION OPERATORS
  // ─────────────────────────────────────────────

  /** map — multiply each value by 2 */
  demoMap(): void {
    of(1, 2, 3, 4, 5)
      .pipe(map((x) => x * 2))
      .subscribe((val) => console.log('map:', val));
    // map: 2  map: 4  map: 6  map: 8  map: 10
  }

  /** switchMap — cancel previous, use latest (simulates search API call) */
  demoSwitchMap(): void {
    const search$ = new Subject<string>();

    search$
      .pipe(
        debounceTime(300),
        distinctUntilChanged(),
        switchMap((term) =>
          of(`"${term}"`).pipe(delay(200))
        )
      )
      .subscribe((val) => console.log('switchMap:', val));

    search$.next('a');
    search$.next('an');
    search$.next('angular'); // only this emits after debounce
    // switchMap:"angular"
  }

  /** mergeMap — run all inner observables concurrently */
  demoMergeMap(): void {
    from(['user1', 'user2', 'user3'])
      .pipe(
        mergeMap((id) =>
          of(`Profile: ${id}`).pipe(delay(100))
        )
      )
      .subscribe((val) => console.log('mergeMap:', val));
    // mergeMap: Profile: user1  mergeMap: Profile: user2  mergeMap: Profile: user3
  }

  /** concatMap — execute inner observables one after another in order */
  demoConcatMap(): void {
    from(['step1', 'step2', 'step3'])
      .pipe(
        concatMap((step) =>
          of(`Done: ${step}`).pipe(delay(300))
        )
      )
      .subscribe((val) => console.log('concatMap:', val));
    // concatMap: Done: step1  concatMap: Done: step2  concatMap: Done: step3
  }

  /** exhaustMap — ignore new values while current inner obs is still active */
  demoExhaustMap(): void {
    const click$ = new Subject<void>();

    click$
      .pipe(
        exhaustMap(() =>
          of('API done').pipe(delay(1000))
        )
      )
      .subscribe((val) => console.log('exhaustMap:', val));

    click$.next(); // processed
    click$.next(); // ignored — first still running
    click$.next(); // ignored — first still running
    // exhaustMap: API done  (only once)
  }

  /** scan — running total emitted after each value */
  demoScan(): void {
    of(10, 20, 30, 40)
      .pipe(scan((acc, val) => acc + val, 0))
      .subscribe((val) => console.log('scan:', val));
    // scan: 10  scan: 30  scan: 60  scan: 100
  }

  /** reduce — emit only the final accumulated result */
  demoReduce(): void {
    of(10, 20, 30, 40)
      .pipe(reduce((acc, val) => acc + val, 0))
      .subscribe((val) => console.log('reduce:', val));
    // reduce: 100
  }

  /** pairwise — emit [prev, curr] on each new value */
  demoPairwise(): void {
    of(1, 2, 3, 4, 5)
      .pipe(pairwise())
      .subscribe((val) => console.log('pairwise:', val));
    // pairwise: [1,2]  pairwise: [2,3]  pairwise: [3,4]  pairwise: [4,5]
  }

  // ─────────────────────────────────────────────
  // 2. FILTERING OPERATORS
  // ─────────────────────────────────────────────

  /** filter — only pass even numbers */
  demoFilter(): void {
    of(1, 2, 3, 4, 5, 6)
      .pipe(filter((x) => x % 2 === 0))
      .subscribe((val) => console.log('filter:', val));
    // filter: 2  filter: 4  filter: 6
  }

  /** take — complete after first 3 emissions */
  demoTake(): void {
    interval(300)
      .pipe(take(3))
      .subscribe({
        next: (val) => console.log('take:', val),
        complete: () => console.log('take: completed'),
      });
    // take: 0  take: 1  take: 2  take: completed
  }

  /** takeLast — emit only last 2 values before completion */
  demoTakeLast(): void {
    of(1, 2, 3, 4, 5)
      .pipe(takeLast(2))
      .subscribe((val) => console.log('takeLast:', val));
    // takeLast: 4  takeLast: 5
  }

  /** takeWhile — keep taking while value is less than 4 */
  demoTakeWhile(): void {
    of(1, 2, 3, 4, 5)
      .pipe(takeWhile((x) => x < 4))
      .subscribe({
        next: (val) => console.log('takeWhile:', val),
        complete: () => console.log('takeWhile: completed'),
      });
    // takeWhile: 1  takeWhile: 2  takeWhile: 3  takeWhile: completed
  }

  /** takeUntil — stop when destroy$ emits */
  demoTakeUntil(): void {
    interval(300)
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (val) => console.log('takeUntil:', val),
        complete: () => console.log('takeUntil: completed'),
      });
    // takeUntil: 0  takeUntil: 1  ...until ngOnDestroy
  }

  /** skip — ignore first 2 values */
  demoSkip(): void {
    of(1, 2, 3, 4, 5)
      .pipe(skip(2))
      .subscribe((val) => console.log('skip:', val));
    // skip: 3  skip: 4  skip: 5
  }

  /** skipWhile — skip values while they are less than 3 */
  demoSkipWhile(): void {
    of(1, 2, 3, 4, 5)
      .pipe(skipWhile((x) => x < 3))
      .subscribe((val) => console.log('skipWhile:', val));
    // skipWhile: 3  skipWhile: 4  skipWhile: 5
  }

  /** skipUntil — skip until timer fires after 1s */
  demoSkipUntil(): void {
    interval(300)
      .pipe(skipUntil(timer(1000)), take(4))
      .subscribe((val) => console.log('skipUntil:', val));
    // skipUntil: 3  skipUntil: 4  skipUntil: 5  skipUntil: 6
  }

  /** first — emit first value greater than 3 */
  demoFirst(): void {
    of(1, 2, 3, 4, 5)
      .pipe(first((x) => x > 3))
      .subscribe((val) => console.log('first:', val));
    // first: 4
  }

  /** last — emit only the last value */
  demoLast(): void {
    of(1, 2, 3, 4, 5)
      .pipe(last())
      .subscribe((val) => console.log('last:', val));
    // last: 5
  }

  /** find — emit first value matching predicate */
  demoFind(): void {
    of(1, 2, 3, 4, 5)
      .pipe(find((x) => x > 3))
      .subscribe((val) => console.log('find:', val));
    // find: 4
  }

  /** distinctUntilChanged — suppress consecutive duplicate values */
  demoDistinctUntilChanged(): void {
    of(1, 1, 2, 2, 3, 1)
      .pipe(distinctUntilChanged())
      .subscribe((val) => console.log('distinctUntilChanged:', val));
    // distinctUntilChanged: 1  2  3  1
  }

  // ─────────────────────────────────────────────
  // 3. TIME-BASED OPERATORS
  // ─────────────────────────────────────────────

  /** debounceTime — wait 300ms after last keystroke before emitting */
  demoDebounceTime(): void {
    const search$ = new Subject<string>();

    search$
      .pipe(debounceTime(300), distinctUntilChanged())
      .subscribe((val) => console.log('debounceTime:', val));

    search$.next('a');
    search$.next('an');
    search$.next('ang'); // only 'ang' emits after 300ms silence
    // debounceTime: ang
  }

  /** throttleTime — emit first, ignore rest for 1s window */
  demoThrottleTime(): void {
    const click$ = new Subject<string>();

    click$
      .pipe(throttleTime(1000))
      .subscribe((val) => console.log('throttleTime:', val));

    click$.next('click1'); // handled
    click$.next('click2'); // ignored
    click$.next('click3'); // ignored
    // throttleTime: click1
  }

  /** auditTime — emit the LAST value at the end of each 500ms window */
  demoAuditTime(): void {
    const move$ = new Subject<number>();

    move$
      .pipe(auditTime(500))
      .subscribe((val) => console.log('auditTime:', val));

    move$.next(10);
    move$.next(20);
    move$.next(30); // 30 will be emitted after 500ms window
    // auditTime: 30
  }

  /** sampleTime — emit most recent value every 500ms */
  demoSampleTime(): void {
    interval(100)
      .pipe(sampleTime(500), take(3))
      .subscribe((val) => console.log('sampleTime:', val));
    // sampleTime: 4  sampleTime: 9  sampleTime: 14
  }

  /** delay — defer emission by 1 second */
  demoDelay(): void {
    of('Hello after 1s')
      .pipe(delay(1000))
      .subscribe((val) => console.log('delay:', val));
    // (after 1s) delay: Hello after 1s
  }

  /** timeout — error if no value within 1s, fallback to 'Timed out' */
  demoTimeout(): void {
    of('data')
      .pipe(
        delay(2000),
        timeout(1000),
        catchError(() => of('Timed out!'))
      )
      .subscribe((val) => console.log('timeout:', val));
    // timeout: Timed out!
  }

  // ─────────────────────────────────────────────
  // 4. COMBINATION OPERATORS
  // ─────────────────────────────────────────────

  /** combineLatest — emit latest pair whenever either source emits */
  demoCombineLatest(): void {
    const num$ = new BehaviorSubject<number>(1);
    const str$ = new BehaviorSubject<string>('A');

    combineLatest([num$, str$])
      .subscribe(([n, s]) => console.log('combineLatest:', n, s));

    num$.next(2); // emits [2, 'A']
    str$.next('B'); // emits [2, 'B']
    // combineLatest: 1 A  combineLatest: 2 A  combineLatest: 2 B
  }

  /** forkJoin — wait for all to complete, emit as a combined object */
  demoForkJoin(): void {
    const user$ = of({ name: 'Alice' }).pipe(delay(300));
    const roles$ = of(['admin', 'editor']).pipe(delay(500));

    forkJoin({ user: user$, roles: roles$ })
      .subscribe((val) => console.log('forkJoin:', JSON.stringify(val)));
    // forkJoin: {"user":{"name":"Alice"},"roles":["admin","editor"]}
  }

  /** merge — subscribe to all, emit values as they arrive (interleaved) */
  demoMerge(): void {
    const a$ = interval(400).pipe(map(() => 'A'), take(3));
    const b$ = interval(300).pipe(map(() => 'B'), take(3));

    merge(a$, b$)
      .subscribe((val) => console.log('merge:', val));
    // merge: B  merge: A  merge: B  merge: A  merge: B  merge: A
  }

  /** concat — subscribe sequentially, preserve order */
  demoConcat(): void {
    concat(of(1, 2, 3), of(4, 5, 6))
      .subscribe((val) => console.log('concat:', val));
    // concat: 1  concat: 2  concat: 3  concat: 4  concat: 5  concat: 6
  }

  /** zip — pair emissions index-by-index from each source */
  demoZip(): void {
    const names$ = of('Alice', 'Bob', 'Carol');
    const scores$ = of(95, 80, 88);

    zip(names$, scores$)
      .subscribe(([name, score]) => console.log('zip:', name, score));
    // zip: Alice 95  zip: Bob 80  zip: Carol 88
  }

  /** withLatestFrom — combine trigger with snapshot of another stream */
  demoWithLatestFrom(): void {
    const trigger$ = new Subject<void>();
    const price$ = new BehaviorSubject<number>(100);

    trigger$
      .pipe(withLatestFrom(price$))
      .subscribe(([, price]) => console.log('withLatestFrom price:', price));

    price$.next(150);
    trigger$.next(); // emits with latest price = 150
    price$.next(200);
    trigger$.next(); // emits with latest price = 200
    // withLatestFrom price: 150  withLatestFrom price: 200
  }

  /** race — subscribe only to the first observable that emits */
  demoRace(): void {
    const slow$ = of('slow').pipe(delay(1000));
    const fast$ = of('fast').pipe(delay(300));

    race(slow$, fast$)
      .subscribe((val) => console.log('race winner:', val));
    // race winner: fast
  }

  /** startWith — prepend a value before the source emits */
  demoStartWith(): void {
    of(1, 2, 3)
      .pipe(startWith(0))
      .subscribe((val) => console.log('startWith:', val));
    // startWith: 0  startWith: 1  startWith: 2  startWith: 3
  }

  // ─────────────────────────────────────────────
  // 5. ERROR HANDLING OPERATORS
  // ─────────────────────────────────────────────

  /** catchError — catch error and return fallback observable */
  demoCatchError(): void {
    throwError(() => new Error('Network error'))
      .pipe(
        catchError((err: Error) => of(`Caught: ${err.message}`))
      )
      .subscribe((val) => console.log('catchError:', val));
    // catchError: Caught: Network error
  }

  /** retry — retry up to 3 times before giving up */
  demoRetry(): void {
    let attempt = 0;

    new Observable<number>((observer) => {
      attempt++;
      console.log(`retry: attempt #${attempt}`);
      if (attempt < 3) {
        observer.error('Temporary failure');
      } else {
        observer.next(200);
        observer.complete();
      }
    })
      .pipe(retry(3))
      .subscribe({
        next: (val) => console.log('retry success:', val),
        error: (err) => console.log('retry failed:', err),
      });
    // retry: attempt #1  retry: attempt #2  retry: attempt #3  retry success: 200
  }

  /** finalize — always runs on complete or error like a finally block */
  demoFinalize(): void {
    of(1, 2, 3)
      .pipe(finalize(() => console.log('finalize: cleanup done')))
      .subscribe((val) => console.log('finalize value:', val));
    // finalize value: 1  finalize value: 2  finalize value: 3  finalize: cleanup done
  }

  // ─────────────────────────────────────────────
  // 6. UTILITY OPERATORS
  // ─────────────────────────────────────────────

  /** tap — log values before and after map without changing the stream */
  demoTap(): void {
    of(1, 2, 3)
      .pipe(
        tap((val) => console.log('tap before map:', val)),
        map((x) => x * 10),
        tap((val) => console.log('tap after map:', val))
      )
      .subscribe((val) => console.log('tap subscribe:', val));
    // tap before map: 1  tap after map: 10  tap subscribe: 10 ...
  }

  /** toArray — collect all values into a single array on completion */
  demoToArray(): void {
    of(1, 2, 3, 4, 5)
      .pipe(toArray())
      .subscribe((val) => console.log('toArray:', val));
    // toArray: [1, 2, 3, 4, 5]
  }

  /** count — count how many even numbers are emitted */
  demoCount(): void {
    of(1, 2, 3, 4, 5, 6)
      .pipe(count((x) => x % 2 === 0))
      .subscribe((val) => console.log('count (evens):', val));
    // count (evens): 3
  }

  /** shareReplay — cache latest value, replay to late subscribers */
  demoShareReplay(): void {
    const shared$ = interval(1000).pipe(
      take(5),
      shareReplay(1)
    );

    shared$.subscribe((val) => console.log('shareReplay sub1:', val));

    setTimeout(() => {
      shared$.subscribe((val) => console.log('shareReplay sub2 (late):', val));
      // sub2 immediately gets the last cached value
    }, 2500);
    // shareReplay sub1: 0  shareReplay sub1: 1  shareReplay sub1: 2
    // shareReplay sub2 (late): 2  shareReplay sub1: 3  shareReplay sub2 (late): 3 ...
  }

  /** share — multicast without replay (hot observable) */
  demoShare(): void {
    const shared$ = interval(1000).pipe(share(), take(4));

    shared$.subscribe((val) => console.log('share sub1:', val));
    shared$.subscribe((val) => console.log('share sub2:', val));
    // share sub1: 0  share sub2: 0  share sub1: 1  share sub2: 1 ...
  }

  // ─────────────────────────────────────────────
  // 7. CONDITIONAL OPERATORS
  // ─────────────────────────────────────────────

  /** iif — subscribe to welcome$ if logged in, else login$ */
  demoIif(): void {
    const isLoggedIn = true;

    iif(
      () => isLoggedIn,
      of('Welcome back!'),
      of('Please log in')
    ).subscribe((val) => console.log('iif:', val));
    // iif: Welcome back!
  }

  // ─────────────────────────────────────────────
  // 8. SUBJECTS
  // ─────────────────────────────────────────────

  /** Subject — no initial value, late subscribers miss past emissions */
  demoSubject(): void {
    const subject = new Subject<number>();

    subject.subscribe((val) => console.log('Subject sub1:', val));

    subject.next(1);
    subject.next(2);

    subject.subscribe((val) => console.log('Subject sub2 (late):', val));

    subject.next(3); // both see this
    subject.complete();
    // Subject sub1: 1  Subject sub1: 2  Subject sub1: 3  Subject sub2 (late): 3
  }

  /** BehaviorSubject — holds current value, late subscribers get it immediately */
  demoBehaviorSubject(): void {
    const bs = new BehaviorSubject<number>(0);

    bs.subscribe((val) => console.log('BehaviorSubject sub1:', val));

    bs.next(1);
    bs.next(2);

    bs.subscribe((val) => console.log('BehaviorSubject sub2 (late):', val)); // gets 2

    bs.next(3);

    console.log('BehaviorSubject getValue():', bs.getValue());
    // sub1: 0  sub1: 1  sub1: 2  sub2 (late): 2  sub1: 3  sub2: 3  getValue: 3
  }

  /** ReplaySubject — replays last N buffered values to new subscribers */
  demoReplaySubject(): void {
    const rs = new ReplaySubject<number>(2); // buffer last 2

    rs.next(1);
    rs.next(2);
    rs.next(3);

    rs.subscribe((val) => console.log('ReplaySubject (late):', val));
    // ReplaySubject (late): 2  ReplaySubject (late): 3  (last 2 replayed)
  }

  // ─────────────────────────────────────────────
  // 9. GROUPING & BUFFERING OPERATORS
  // ─────────────────────────────────────────────

  /** bufferTime — collect emissions into arrays every 1 second */
  demoBufferTime(): void {
    interval(200)
      .pipe(bufferTime(1000), take(3))
      .subscribe((val) => console.log('bufferTime:', val));
    // bufferTime: [0,1,2,3,4]  bufferTime: [5,6,7,8,9]  bufferTime: [10,11,12,13,14]
  }

  /** groupBy — group employees by department */
  demoGroupBy(): void {
    of(
      { name: 'Alice', dept: 'engineering' },
      { name: 'Bob',   dept: 'design' },
      { name: 'Carol', dept: 'engineering' },
      { name: 'Dave',  dept: 'design' }
    )
      .pipe(
        groupBy((p) => p.dept),
        mergeMap((group$) =>
          group$.pipe(
            toArray(),
            map((members) => ({ dept: group$.key, members }))
          )
        )
      )
      .subscribe((val) => console.log('groupBy:', JSON.stringify(val)));
    // groupBy: {"dept":"engineering","members":[{"name":"Alice"...},{"name":"Carol"...}]}
    // groupBy: {"dept":"design","members":[{"name":"Bob"...},{"name":"Dave"...}]}
  }

  // ─────────────────────────────────────────────
  // 10. CREATION HELPERS
  // ─────────────────────────────────────────────

  /** of — emit fixed values synchronously */
  demoOf(): void {
    of(10, 20, 30)
      .subscribe((val) => console.log('of:', val));
    // of: 10  of: 20  of: 30
  }

  /** from — convert array into individual emissions */
  demoFrom(): void {
    from(['apple', 'banana', 'cherry'])
      .subscribe((val) => console.log('from:', val));
    // from: apple  from: banana  from: cherry
  }

  /** interval — emit incrementing numbers every 500ms */
  demoInterval(): void {
    interval(500)
      .pipe(take(5))
      .subscribe((val) => console.log('interval:', val));
    // interval: 0  interval: 1  interval: 2  interval: 3  interval: 4
  }

  /** timer — wait 1s then emit every 500ms */
  demoTimer(): void {
    timer(1000, 500)
      .pipe(take(4))
      .subscribe((val) => console.log('timer:', val));
    // (after 1s) timer: 0  timer: 1  timer: 2  timer: 3
  }
}