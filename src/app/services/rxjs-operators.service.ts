import { Injectable, OnDestroy } from '@angular/core';
import {
  Observable, Subject, BehaviorSubject, ReplaySubject,
  of, from, interval, timer, combineLatest, forkJoin,
  merge, concat, zip, throwError, race, iif,
} from 'rxjs';
import {
  map, filter, tap, take, skip, takeUntil, takeWhile,
  switchMap, mergeMap, concatMap, exhaustMap,
  debounceTime, throttleTime, distinctUntilChanged,
  catchError, retry, finalize,
  withLatestFrom, startWith,
  scan, reduce, bufferTime,
  shareReplay, share,
  delay, timeout, first, last, find,
  pairwise, toArray, count,
  groupBy, partition,
  skipUntil, skipWhile, takeLast,
  auditTime, sampleTime,
} from 'rxjs/operators';

// ─────────────────────────────────────────────────────────────────────────────
// HOW TO USE IN ANY COMPONENT:
//
//   import { RxjsOperatorsService } from './rxjs-operators.service';
//
//   @Component({ ... })
//   export class MyComponent implements OnInit, OnDestroy {
//     private rxjs = inject(RxjsOperatorsService);
//     private destroy$ = new Subject<void>();
//
//     ngOnInit(): void {
//       this.rxjs.demoMap().subscribe(val => console.log(val));
//       this.rxjs.demoForkJoin().subscribe(val => console.log(val));
//     }
//     ngOnDestroy(): void { this.destroy$.next(); this.destroy$.complete(); }
//   }
// ─────────────────────────────────────────────────────────────────────────────

@Injectable({ providedIn: 'root' })
export class RxjsOperatorsService implements OnDestroy {

  private destroy$ = new Subject<void>();

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  // ───────────────────────────────────────────
  // 1. TRANSFORMATION OPERATORS
  // ───────────────────────────────────────────

  /** map — transform each emitted value */
  demoMap(): Observable<number> {
    return of(1, 2, 3, 4, 5).pipe(
      map((x) => x * 2)
      // emits: 2, 4, 6, 8, 10
    );
  }

  /** switchMap — cancel previous inner obs, subscribe to new (use for search) */
  demoSwitchMap(term$: Observable<string>): Observable<string> {
    return term$.pipe(
      debounceTime(300),
      distinctUntilChanged(),
      switchMap((term) =>
        of(`Server result for: "${term}"`).pipe(delay(200))
        // real usage: this.http.get(`/api/search?q=${term}`)
      )
    );
  }

  /** mergeMap — run all inner observables concurrently (no order guarantee) */
  demoMergeMap(userIds: string[]): Observable<string> {
    return from(userIds).pipe(
      mergeMap((id) =>
        of(`Profile loaded: ${id}`).pipe(delay(Math.random() * 400))
      )
    );
  }

  /** concatMap — queue inner observables, preserve strict order */
  demoConcatMap(steps: string[]): Observable<string> {
    return from(steps).pipe(
      concatMap((step) =>
        of(`✔ Completed: ${step}`).pipe(delay(300))
      )
    );
  }

  /** exhaustMap — ignore new triggers while inner observable is active (e.g. login) */
  demoExhaustMap(trigger$: Observable<void>): Observable<string> {
    return trigger$.pipe(
      exhaustMap(() =>
        of('API response').pipe(delay(1500))
        // any trigger fired during 1.5s is silently dropped
      )
    );
  }

  /** scan — running accumulation, emits on every step (e.g. cart total) */
  demoScan(): Observable<number> {
    return of(10, 20, 30, 40).pipe(
      scan((acc, val) => acc + val, 0)
      // emits: 10, 30, 60, 100
    );
  }

  /** reduce — like scan but emits only the final accumulated value */
  demoReduce(): Observable<number> {
    return of(10, 20, 30, 40).pipe(
      reduce((acc, val) => acc + val, 0)
      // emits: 100
    );
  }

  /** pairwise — emit [previous, current] as a pair on each value */
  demoPairwise(): Observable<[number, number]> {
    return of(1, 2, 3, 4, 5).pipe(
      pairwise()
      // emits: [1,2], [2,3], [3,4], [4,5]
    );
  }

  // ───────────────────────────────────────────
  // 2. FILTERING OPERATORS
  // ───────────────────────────────────────────

  /** filter — only pass values matching the predicate */
  demoFilter(): Observable<number> {
    return of(1, 2, 3, 4, 5, 6).pipe(
      filter((x) => x % 2 === 0)
      // emits: 2, 4, 6
    );
  }

  /** take — complete after emitting N values */
  demoTake(n = 3): Observable<number> {
    return interval(500).pipe(
      take(n)
      // emits: 0, 1, 2 — then completes
    );
  }

  /** takeLast — emit only the last N values before completion */
  demoTakeLast(): Observable<number> {
    return of(1, 2, 3, 4, 5).pipe(
      takeLast(2)
      // emits: 4, 5
    );
  }

  /** takeWhile — take values while condition is true */
  demoTakeWhile(): Observable<number> {
    return of(1, 2, 3, 4, 5).pipe(
      takeWhile((x) => x < 4)
      // emits: 1, 2, 3
    );
  }

  /** takeUntil — complete when the notifier emits (best for component cleanup) */
  demoTakeUntil(): Observable<number> {
    return interval(300).pipe(
      takeUntil(this.destroy$)
    );
  }

  /** skip — ignore the first N values */
  demoSkip(): Observable<number> {
    return of(1, 2, 3, 4, 5).pipe(
      skip(2)
      // emits: 3, 4, 5
    );
  }

  /** skipWhile — skip values while condition is true */
  demoSkipWhile(): Observable<number> {
    return of(1, 2, 3, 4, 5).pipe(
      skipWhile((x) => x < 3)
      // emits: 3, 4, 5
    );
  }

  /** skipUntil — skip until the notifier emits */
  demoSkipUntil(): Observable<number> {
    const gate$ = timer(1000);
    return interval(300).pipe(
      skipUntil(gate$),
      take(4)
    );
  }

  /** first — emit only the first value matching the predicate */
  demoFirst(): Observable<number> {
    return of(1, 2, 3, 4, 5).pipe(
      first((x) => x > 3)
      // emits: 4
    );
  }

  /** last — emit only the last value before completion */
  demoLast(): Observable<number> {
    return of(1, 2, 3, 4, 5).pipe(
      last()
      // emits: 5
    );
  }

  /** find — emit first value matching predicate, then complete */
  demoFind(): Observable<number | undefined> {
    return of(1, 2, 3, 4, 5).pipe(
      find((x) => x > 3)
      // emits: 4
    );
  }

  /** distinctUntilChanged — suppress consecutive duplicate values */
  demoDistinctUntilChanged(): Observable<number> {
    return of(1, 1, 2, 2, 3, 1).pipe(
      distinctUntilChanged()
      // emits: 1, 2, 3, 1
    );
  }

  // ───────────────────────────────────────────
  // 3. TIME-BASED OPERATORS
  // ───────────────────────────────────────────

  /** debounceTime — wait for silence then emit last value (search boxes) */
  demoDebounceTime(input$: Observable<string>): Observable<string> {
    return input$.pipe(
      debounceTime(300),
      distinctUntilChanged()
      // only emits 300ms after typing stops
    );
  }

  /** throttleTime — rate-limit: emit first value, suppress for duration */
  demoThrottleTime(clicks$: Observable<void>): Observable<void> {
    return clicks$.pipe(
      throttleTime(1000)
      // at most one event handled per second
    );
  }

  /** auditTime — emit the LAST value at the end of each time window */
  demoAuditTime(moves$: Observable<number>): Observable<number> {
    return moves$.pipe(
      auditTime(500)
      // emits last position every 500ms
    );
  }

  /** sampleTime — emit most recent value every N ms regardless of activity */
  demoSampleTime(): Observable<number> {
    return interval(100).pipe(
      sampleTime(500),
      take(3)
    );
  }

  /** delay — defer each emission by N milliseconds */
  demoDelay(ms = 1000): Observable<string> {
    return of('Delayed hello').pipe(
      delay(ms)
    );
  }

  /** timeout — throw error if no value arrives within the deadline */
  demoTimeout(source$: Observable<unknown>, ms = 3000): Observable<unknown> {
    return source$.pipe(
      timeout(ms),
      catchError(() => of('⏱ Request timed out'))
    );
  }

  // ───────────────────────────────────────────
  // 4. COMBINATION OPERATORS
  // ───────────────────────────────────────────

  /** combineLatest — emit array of latest values when any source emits */
  demoCombineLatest(
    a$: Observable<number>,
    b$: Observable<string>
  ): Observable<[number, string]> {
    return combineLatest([a$, b$]);
    // emits every time a$ or b$ changes
  }

  /** forkJoin — wait for ALL sources to complete, emit last values (Promise.all) */
  demoForkJoin(): Observable<{ user: object; roles: string[] }> {
    const user$ = of({ id: 1, name: 'Alice' }).pipe(delay(300));
    const roles$ = of(['admin', 'editor']).pipe(delay(500));
    return forkJoin({ user: user$, roles: roles$ });
    // emits once when both complete
  }

  /** merge — subscribe to all sources, emit values as they arrive */
  demoMerge(): Observable<string> {
    const a$ = interval(400).pipe(map(() => 'A'), take(3));
    const b$ = interval(300).pipe(map(() => 'B'), take(3));
    return merge(a$, b$);
    // interleaved: B, A, B, A, B, A
  }

  /** concat — subscribe sequentially, next starts only after previous completes */
  demoConcat(): Observable<number> {
    return concat(
      of(1, 2, 3),
      of(4, 5, 6)
    );
    // emits: 1, 2, 3, 4, 5, 6
  }

  /** zip — pair corresponding emissions index-by-index from each source */
  demoZip(): Observable<[string, number]> {
    const names$ = of('Alice', 'Bob', 'Carol');
    const scores$ = of(95, 80, 88);
    return zip(names$, scores$);
    // emits: ['Alice',95], ['Bob',80], ['Carol',88]
  }

  /** withLatestFrom — combine trigger with snapshot of another stream */
  demoWithLatestFrom(
    trigger$: Observable<void>,
    price$: Observable<number>
  ): Observable<number> {
    return trigger$.pipe(
      withLatestFrom(price$),
      map(([, price]) => price)
      // emits current price whenever trigger fires
    );
  }

  /** race — subscribe only to the first source that emits */
  demoRace(): Observable<string> {
    const slow$ = of('slow server').pipe(delay(1000));
    const fast$ = of('fast server').pipe(delay(300));
    return race(slow$, fast$);
    // emits: 'fast server'
  }

  /** startWith — prepend initial value(s) before source emits */
  demoStartWith(): Observable<number | string> {
    return of(1, 2, 3).pipe(
      startWith('start')
      // emits: 'start', 1, 2, 3
    );
  }

  // ───────────────────────────────────────────
  // 5. ERROR HANDLING OPERATORS
  // ───────────────────────────────────────────

  /** catchError — intercept errors and return a fallback observable */
  demoCatchError(): Observable<string> {
    return throwError(() => new Error('Network error')).pipe(
      catchError((err: Error) => of(`Fallback: ${err.message}`))
    );
  }

  /** retry — automatically re-subscribe on error up to N times */
  demoRetry(maxRetries = 3): Observable<number> {
    let attempt = 0;
    return new Observable<number>((observer) => {
      attempt++;
      console.log(`Attempt #${attempt}`);
      if (attempt < maxRetries) {
        observer.error('Temporary failure');
      } else {
        observer.next(200);
        observer.complete();
      }
    }).pipe(
      retry(maxRetries)
    );
  }

  /** finalize — always run on complete or error, like a finally block */
  demoFinalize(): Observable<number> {
    return of(1, 2, 3).pipe(
      finalize(() => console.log('Cleanup: stream ended'))
    );
  }

  // ───────────────────────────────────────────
  // 6. UTILITY OPERATORS
  // ───────────────────────────────────────────

  /** tap — inspect/log values without modifying the stream */
  demoTap(): Observable<number> {
    return of(1, 2, 3).pipe(
      tap((val) => console.log('before map:', val)),
      map((x) => x * 10),
      tap((val) => console.log('after map:', val))
    );
  }

  /** toArray — collect all values into a single array on completion */
  demoToArray(): Observable<number[]> {
    return of(1, 2, 3, 4, 5).pipe(
      toArray()
      // emits: [1, 2, 3, 4, 5]
    );
  }

  /** count — count emissions matching predicate, emit total on completion */
  demoCount(): Observable<number> {
    return of(1, 2, 3, 4, 5, 6).pipe(
      count((x) => x % 2 === 0)
      // emits: 3  (three even numbers)
    );
  }

  /** shareReplay — multicast + replay last N values to late subscribers (HTTP cache) */
  demoShareReplay(): Observable<number> {
    const shared$ = interval(1000).pipe(
      take(5),
      shareReplay(1)
    );
    shared$.subscribe((val) => console.log('sub1:', val));
    setTimeout(() => {
      shared$.subscribe((val) => console.log('sub2 (late — gets cached):', val));
    }, 2500);
    return shared$;
  }

  /** share — multicast without replay (hot observable, no buffer) */
  demoShare(): Observable<number> {
    const shared$ = interval(1000).pipe(
      share(),
      take(4)
    );
    shared$.subscribe((val) => console.log('share sub1:', val));
    shared$.subscribe((val) => console.log('share sub2:', val));
    return shared$;
    // both subscribers share a single interval execution
  }

  // ───────────────────────────────────────────
  // 7. CONDITIONAL OPERATORS
  // ───────────────────────────────────────────

  /** iif — conditionally subscribe to one of two observables */
  demoIif(isLoggedIn: boolean): Observable<string> {
    return iif(
      () => isLoggedIn,
      of('Welcome back!'),
      of('Please log in')
    );
  }
  

  // ───────────────────────────────────────────
  // 8. SUBJECTS
  // ───────────────────────────────────────────

  /** Subject — plain multicast, no initial value, late subs miss past values */
  demoSubject(): Subject<number> {
    const subject = new Subject<number>();

    subject.subscribe((val) => console.log('sub1:', val));
    subject.next(1);   // sub1 gets 1
    subject.next(2);   // sub1 gets 2

    subject.subscribe((val) => console.log('sub2 (late):', val));
    subject.next(3);   // both sub1 and sub2 get 3

    return subject;
  }

  /** BehaviorSubject — holds current value, new subscribers get it immediately */
  demoBehaviorSubject(): BehaviorSubject<number> {
    const bs = new BehaviorSubject<number>(0);  // initial value: 0

    bs.subscribe((val) => console.log('sub1:', val));  // gets 0 immediately
    bs.next(1);
    bs.next(2);

    bs.subscribe((val) => console.log('sub2 (late):', val)); // gets 2 immediately
    bs.next(3);  // both get 3

    console.log('current sync value:', bs.getValue()); // 3

    return bs;
  }

  /** ReplaySubject — replays last N buffered values to any new subscriber */
  demoReplaySubject(bufferSize = 2): ReplaySubject<number> {
    const rs = new ReplaySubject<number>(bufferSize);

    rs.next(1);
    rs.next(2);
    rs.next(3);

    rs.subscribe((val) => console.log('late sub gets:', val));
    // emits: 2, 3  (last 2 buffered)

    return rs;
  }

  // ───────────────────────────────────────────
  // 9. GROUPING & BUFFERING OPERATORS
  // ───────────────────────────────────────────

  /** bufferTime — collect values into arrays over fixed time windows */
  demoBufferTime(): Observable<number[]> {
    return interval(200).pipe(
      bufferTime(1000),
      take(3)
      // each emission is an array of ~5 values collected in 1s
    );
  }

  /** groupBy — split stream into group observables keyed by a field */
  demoGroupBy(): Observable<{ dept: string; members: object[] }> {
    return of(
      { name: 'Alice', dept: 'engineering' },
      { name: 'Bob',   dept: 'design' },
      { name: 'Carol', dept: 'engineering' },
      { name: 'Dave',  dept: 'design' }
    ).pipe(
      groupBy((person) => person.dept),
      mergeMap((group$) =>
        group$.pipe(
          toArray(),
          map((members) => ({ dept: group$.key, members }))
        )
      )
    );
  }

  // ───────────────────────────────────────────
  // 10. CREATION HELPERS
  // ───────────────────────────────────────────

  /** of — emit a fixed set of values synchronously */
  demoOf(): Observable<number> {
    return of(10, 20, 30);
    // emits: 10, 20, 30
  }

  /** from — convert array / promise / iterable into an observable */
  demoFrom(): Observable<number> {
    return from([1, 2, 3, 4, 5]);
    // emits each item individually
  }

  /** interval — emit incrementing number every N ms */
  demoInterval(): Observable<number> {
    return interval(1000).pipe(take(5));
    // emits: 0, 1, 2, 3, 4  (one per second)
  }

  /** timer — emit once after delay, or repeatedly with initial delay + period */
  demoTimer(): Observable<number> {
    return timer(1000, 500).pipe(take(5));
    // waits 1s, then emits every 500ms: 0, 1, 2, 3, 4
  }
}
