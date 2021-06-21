# Spark study 
참조 서적: Spark in Action (https://www.manning.com/books/spark-in-action) <br>
참조 github: https://github.com/spark-in-action/first-edition
<details close>
<summary>Original readme</summary>
  <h1>Spark in Action book repository</h1>
  <h4> Current edition: Manning Early Access Program (MEAP)</h4>

  The MEAP publishing date is **2015.04.04.**  
  Manning's book forum: [https://forums.manning.com/forums/spark-in-action](https://forums.manning.com/forums/spark-in-action)

  The repo contains book listings organized by chapter and programming language (if applicable):

  ```
  ch02
    ├── scala
    │    ├── ch02-listings.scala
    │    ├── scala-file.scala
    │    └── ...
    ├── java
    │    ├── Class1.java
    │    ├── Class2.java
    │    └── ...
    ├── python
    │    ├── ch02-listings.py
    │    ├── python-file.py
    │    └── ...
  ch03
    ├── scala
    │    ├── ch03-listings.scala
    │    ├── scala-file.scala
    │    └── ...
    ├── java
    │    ├── Class1.java
    │    ├── Class2.java
    │    └── ...
    ├── python
    │    ├── ch03-listings.py
    │    ├── python-file.py
    │    └── ...

  ```

  We tried to organize the listings so that you have minimum distractions while going through the book.  
  We hope you'll find the content useful and that you'll have fun reading the book and going through examples.  

  As part of Manning's "in action" series, it's a hands-on, tutorial-style book.  

  Thank you,  
  [Petar Zečević](http://twitter.com/p_zecevic) and [Marko Bonaći](http://twitter.com/markobonaci)
</details>


## 1장 First steps
<details close>
<summary><b>chapter 1</b> Introduction to apache spark</summary>
  <br>
  <blockquote>
  <details close>
  <summary>1.1 What is spark?</summary>
    <blockquote>
      
- **hadoop**: open source, distributed, Java computation framework consisting of the (1) **Hadoop Distributed File System(HDFS**) and (2) **MapReduce**, its execution engine. 
Hadoop solved the three main problems facing any distributed data-processing endeavor:
    - Parallelization— How to parallelize the computation (전체 연산을 잘게 나누어 동시에 처리하는 방법)
    - Distribution— How to distribute the data (데이터를 여러 노드로 분산하는 방법)
    - Fault-tolerance— How to handle component failure (분산 컴포넌트 장애에 대응하는 방법)

    → 하둡의 한계: 맵리듀스 잡의 결과를 다른 잡에서 사용하려면 먼저 이 결과를 HDFS에 저장해야 한다. 따라서 맵리듀스는 이전 잡의 결과가 다음 작업의 입력이 되는 반복 알고리즘에는 본질적으로 맞지 않다. 

- **Spark**와 hadoop의 차이: spark는 메모리에 아주 큰 데이터를 keep 하는 것을 가능하게 한다. 즉, 맵리듀스처럼 잡에 필요한 데이터를 디스크에서 매번 가져오는 대신(→ 하둡의 방법), 데이터를 메모리에 캐시로 저장하는 인-메모리 실행 모델에 있다. 이를 통해 스파크 프로그램은 맵리듀스보다 약 100배 더 빠른 속도로 같은 작업을 수행할 수 있다.
- 일부 애플리케이션은 스파크를 사용하기에 적합하지 않다. 스파크에서는 분산 아키텍처 때문에 처리 시간에 약간의 오버헤드가 필연적으로 발생한다. 대량의 데이터를 다룰 때는 오버헤드가 무시할 수 있는 수준이지만, 단일 머신에서도 충분히 처리할 수 있는 데이터셋을 다룰 때는 작은 데이터셋의 연산에 최적화된 다른 프레임워크(e.g. RDBMS)를 사용하는 것이 더 효율적이다. + 하지만 현대의 데이터 크기는 계속 증가하는 추세며, 언제가는 RDBMS와 스크립트 성능이 개선되는 속도를 추월할지도 모른다.
    </blockquote>
  </details>
    
  <details close>
  <summary>1.2 Spark components</summary>
    <blockquote><br>
스파크는 여러 특수한 목적에 맞게 설계된 다양한 컴포넌트로 구성된다.<br>
→ 스파크 core, 스파크 SQL, 스파크 Streaming, 스파크 GraphX, 스파크 MLlib <br><br>
<img width="629" alt="Screen Shot 2021-06-10 at 9 33 02 PM" src="https://user-images.githubusercontent.com/43725183/121525578-78936300-ca33-11eb-8dae-549e2a93f21a.png">

- **스파크 core**: 스파크 코어는 스파크 잡과 다른 스파크 컴포넌트에 필요한 기본 기능을 제공한다. RDD는 분산 데이터 컬렉션(즉, 데이터셋)을 추상화한 객체로 데이터셋에 적용할 수 있는 연산 및 변환 메서드를 함께 제공한다. RDD는 노드에 장애가 발생해도 데이터셋을 재구성할 수 있는 복원성을 갖추었다. broadcast variable(공유변수)와 accumulator(누적변수)를 사용해 컴퓨팅 노드 간에 정보를 공유할 수 있다. 이외에도 스파크 코어에는 네트워킹, 보안, 스케줄링 및 데이터 셔플링 등 기본 기능이 구현되어 있다.
- **스파크 SQL**
- **스파크 Streaming**
- **스파크 GraphX**
- **스파크 MLlib**
    </blockquote>
  </details>
    
  <details close>
  <summary>1.3 Spark program flow</summary>
    <blockquote><br>
예제를 통해 스파크 프로그램이 어떻게 실행되는지 알아보자.<br>
**[예제] 로그파일로부터 OutOfMemoryError 오류가 몇건 발생했는지 분석하기**

1. 노드 세 개로 구성된 하둡 클러스터에 300MB 크기의 로그 파일을 저장되어 있는 상황
→ HDFS는 자동으로 128MB크기의 chunck로 분할하고(하둡에서는 block이라는 용어를 사용한다.), 각 **블록(block)**을 클러스터의 여러 노드에 나누어 저장한다. 
→ 이 예제와 직접적인 관련은 없지만, HDFS는 노드에 저장한 각 블록을 다른 노드 두 개에 복제한다. (복제 계수가 기본 값은 3으로 설정된 경우)<br><img width="632" alt="Screen Shot 2021-06-10 at 9 55 04 PM" src="https://user-images.githubusercontent.com/43725183/121528532-85fe1c80-ca36-11eb-93e1-ba484fc4f839.png">

2. HDFS에 저장된 텍스트 파일을 메모리에 로드한다. 
→ 먼저 스파크 셸을 시작하고, 스파크 클러스터에 연결하기.
→ **데이터 지역성**(data locality, 즉 디스크에 저장된 파일이 모두 해당 노드 각각에서 RAM으로 로드되는 것/대량의 데이터를 네트워크로 전송해야하는 상황을 만들지 않기 위해!)을 최대한 달성하려고 로그 파일의 각 블록이 저장된 위치를 하둡에게 요청한 후, 모든 블록을 클러스터 노드의 RAM 메모리로 전송한다. 
→ RAM에 로딩된 블록을 **partition**이라고 한다. 이 파티션 집합이 바로 RDD가 참조하는 분산 컬렉션이다. RDD API를 사용해 RDD의 컬렉션을 필터링하고, 사용자 함수로 매핑하고, 누적 값 하나로 리듀스하고, 두 RDD를 서로 빼거나 교차하거나 결합하는 등 다양한 작업을 실행할 수 있다. 

    ```scala
    val lines = sc.textFile("hdfs://path/to/the/file")
    ```
      <img width="632" alt="Screen Shot 2021-06-10 at 9 55 21 PM" src="https://user-images.githubusercontent.com/43725183/121528576-90b8b180-ca36-11eb-8e1d-beca8097e67a.png">
      
3. 컬렉션이 OutOfMemoryError 문자열을 포함한 줄만 필터링
→ 필터링이 완료되면 RDD에는 분석에 필요한 데이터만 포함된다. 
→ 마지막으로 로그에 남은 줄 개수를 센다. 

    ```scala
    val oomLines = lines.filter(l => l.contains("OutOfMemoryError")).cache()
    val result = oomLines.count()
    ```
      <img width="634" alt="Screen Shot 2021-06-10 at 9 55 38 PM" src="https://user-images.githubusercontent.com/43725183/121528614-99a98300-ca36-11eb-85ff-f57f68c46ae6.png">
    </blockquote>
  </details>
    
  <details close>
  <summary>1.4 Spark ecosystem</summary>
    <blockquote><br>
      
      
* 하둡 생태계: 인터페이스 도구, 분석 도구, 클러스터 관리 도구, 인프라 도구로 구성되어 있다.
      
<br><img width="643" alt="Screen Shot 2021-06-10 at 9 45 04 PM" src="https://user-images.githubusercontent.com/43725183/121527201-1fc4ca00-ca35-11eb-957f-e701657ebda6.png">
- 하둡생태계의 일부 컴포넌트는 스파크 컴포넌트로 대체될 수 있다. <br>
e.g. Giraph → 스파크 GraphX, <br>
Mahout → 스파크 MLlib, <br>
Strom → 스파크 스트리밍, <br>
pig, sqoop → 스파크 코어, 스파크 SQL
- 하둡 생태계의 인프라 및 관리 도구들은 스파크로 대체할 수 없다. e.g. Oozie(→ Job scheduling), HBase, Zookeeper
- 스파크에 반드시 HDFS 스토리지를 사용할 필요는 없다. 스파크는 HDFS 외에도 **아마존 S3 버킷**이나 **일반 파일 시스템**에 저장된 데이터를 처리할 수 있다.
    </blockquote>
  </details>
    
  <details close>
  <summary>1.5 Setting up the spark-in-action VM</summary>
    <blockquote><br>

* vagrant와 virtualbox를 이용
    </blockquote>
  </details>
  </blockquote>
</details>


<details close>
<summary><b>chapter 2</b> Spark fundamentals</summary>
  <br><blockquote>
  <details close>
  <summary>2.1. Using the spark-in-action VM</summary>
    
```bash
$ vagrant up #가상 머신 시작
$ vagrant ssh -- -l spark #ssh 로그인

$ git clone https://github.com/spark-in-action/first-edition #github repository clone
```

cf. **하둡 사용하기**: `hadoop fs`로 시작한다. e.g. `hadoop fs -ls /user` (/user 폴더내 list 출력) → [http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html) 참고하기
  </details>
  <details close>
  <summary>2.2. Using Spark shell and writing your first Spark program</summary>

- license 파일을 읽어들이고, 이 파일에 포함된 BSD 문자열의 수를 세보자.

    ```python
    #license 파일을 읽기
    licLines = sc.textFile("/usr/local/spark/LICENSE")
    lineCnt = licLines.count()

    #BSD 문자열 필터링
    bsdLines = licLines.filter(lambda line: "BSD" in line)
    bsdLines.count()

    #BSD 문자열 출력
    bsdLines.foreach(lambda bLine: print(bLine))
    ```

- RDD의 개념: RDD는 스파크의 기본 추상화 객체로 다음과 같은 성질이 있다.
    - **불변성(immutable)**: 읽기 전용
    → RDD는 데이터를 조작할 수 있는 다양한 변환 연산자를 제공하지만, 변환 연산자는 항상 새로운 RDD를 생성한다.
    - **복원성(resilient)**: 장애 내성
    - **분산(distributed):** 노드 한개 이상에 저장된 데이터셋
- **RDD lineage**: 앞의 예제에서 LICENSE 파일을 로드해 `licLines` RDD를 생성했다. 그 다음 `licLines` RDD에 `filter` 함수를 적용해 새로운 `bsdLines` RDD를 생성했다. 이처럼 RDD에 적용된 변환 연산자와 그 적용 순서를 RDD lineage라고 한다.
  </details>
  <details close>
  <summary>2.3. Basic RDD actions and transformations</summary><br>
    
1. **transformation**: RDD를 조작해 새로운 RDD를 생성한다. e.g. filter, map
2. **actions**: 계산 결과를 반환하거나 RDD 요소에 특정 작업을 수행하려고 실제 계산을 시작하는 역할을 한다. e.g. count, foreach, collect, first

> 변환 연산자의 **지연 실행(lazy evaluation)**: 변환 연산자의 지연 실행은 행동 연산자를 호출하기 전까지는 변환 연산자의 계산을 실제로 실행하지 않는 것을 의미한다. RDD에 행동 연산자가 호출되면 스파크는 해당 RDD의 lineage를 살펴보고, 이를 바탕으로 실행해야 하는 연산 그래프를 작성해서 행동 연산자를 계산한다.  **결국 변환 연산자는 행동 연산자를 호출했을때 무슨 연산이 어떤 순서로 실행되어야 할지 알려주는 일종의 설계도라고 할 수 있다.** 

---

- **`map`**: RDD의 모든 요소에 임의의 함수를 적용할 수 있는 변환 연산자.

    ```python
    numbers = sc.parallelize(range(10, 51, 10))  #numbers = sc.makeRDD(range(10, 51, 10))도 가능.
    numbers.foreach(lambda x: print(x))
    numbersSquared = numbers.map(lambda num: num * num)
    numbersSquared.foreach(lambda x: print(x))
    ```

    `parallelize`는 sequential 객체를 받아 이 sequential 객체의 요소로 구성된 새로운 RDD를 만든다. 이 과정에서 sequential 객체의 요소는 여러 스파크의 executor로 **분산**된다. 
    (→ `print`결과를 보면 sequential 하지 않다. )

    ```python
    reversed = numbersSquared.map(lambda x: str(x)[::-1])
    reversed.foreach(lambda x: print(x))
    ```

- **`distinct`** and **`flatMap`**

    ```
    ##client-ids.log
    15,16,20,20
    77,80,94
    94,98,16,31
    31,15,20
    ```

    `flatMap`(주어진 모든 요소에 대해 연산을 진행하고, 반환한 배열의 중첩구조를 한 단계 제거하고 모든 요소를 단일 컬렉션으로 병합한다. )을 사용하면 각 요소별로 split을 진행한 결과를 하나의 array로 반환한다. 

    ```python
    lines = sc.textFile("Downloads/client-ids.log")

    ids = lines.flatMap(lambda x: x.split(","))
    ids.collect()   #리스트를 반환한다. 
    ids.first()

    intIds = ids.map(lambda x: int(x))
    intIds.collect()

    uniqueIds = intIds.distinct()
    uniqueIds.collect()
    finalCount  = uniqueIds.count()
    finalCount  #8
    ```

- **`sample`**, **`take`**, **`takeSample`** 연산으로 RDD 일부 요소 가져오기: `sample`은 변환연산자(transformation)이지만, `take`와 `takeSample`은 행동연산자(Actions)이다.

    ```python
    ##비복원 샘플링
    s = uniqueIds.sample(False, 0.3)  #withReplacement=False
    s.count()
    s.collect()

    ##복원 샘플링
    swr = uniqueIds.sample(True, 0.5)
    swr.count
    swr.collect()

    #확률값대신 정확한 개수로 RDD 요소 샘플링하기
    taken = uniqueIds.takeSample(False, 5)  #list 반환
    uniqueIds.take(3)
    ```

    `take`는 지정된 개수의 요소를 모을 때까지 RDD의 파티션을 하나씩 처리해 결과를 반환한다. → 결국 연산이 전혀 분산되지 않는다는 의미이다. 여러 파티션의 요소를 빠르게 가져오고 싶다면 드라이버의 메모리를 넘지 않도록 요소 개수를 적당히 줄이고 `collect` 연산자를 사용하자.
  </details>
  <details close>
  <summary>2.4. Double RDD functions</summary>
    
* Double(cf. float) 객체만 사용해서 RDD를 구성하면 implicit conversion을 통해 몇 가지 추가 함수를 사용할 수 있다.

    ```python
    intIds.mean()
    intIds.sum()

    intIds.stats()
    intIds.variance()
    intIds.stdev()

    intIds.histogram([1.0, 50.0, 100.0])  #1과 50사이에 몇개, 50과 100사이에 몇개가 있는지 반환
    intIds.histogram(3)  #3개의 경계로 나눴을때 (1) 경계값과, (2) 구간에 존재하는 sample의 수를 반환한다. 
    ```

* 근사 합계 및 평균계산: 지정된 제한시간 동안 근사합계 또는 근사 평균을 계산한다. 제한시간을 인자로 넣어준다.

    ```python
    intIds.sumApprox(1)
    intIds.meanApprox(1)
    ```
  </details></blockquote>
</details>
  
<details close>
<summary><b>chapter 3</b> Writing spark application</summary>
  <br><blockquote>
  <details close>
  <summary>3.1. Generating a new Spark project in Eclipse</summary

<br>Eclipse 대신 pycharm 사용하기
  </details>
  <details close>
  <summary>3.2. Developing the application</summary><br>
    
**[예제] 한 게임 회사의 모든 직원 명단과 각 직원이 수행한 푸시 횟수를 담은 일일 리포트를 개발해보자.**

1. 데이터셋 준비

    ```bash
    $ mkdir -p $HOME/sia/github-archive
    $ cd $HOME/sia/github-archive
    $ wget http://data.githubarchive.org/2015-03-01-{0..23}.json.gz

    $ gunzip *

    $ head -1 2015-03-01-0.json| jq '.'
    ```
    <img width="400" alt="Screen Shot 2021-06-16 at 7 32 50 PM" src="https://user-images.githubusercontent.com/43725183/122204059-a5cf8d80-ced9-11eb-9334-c0b2309e6910.png">

    > actor.login이 "treydock"인 누군가가 2015년 3월 1일 자정(created_at)에 "development "라는 저장소 브랜치를 생성한 기록이다. (깃허브 API 문서 참조: [https://developer.github.com/v3/activity/events/types/](https://developer.github.com/v3/activity/events/types/))

2. JSON 로드: `read.json()` 은 한 줄당 JSON 객체 하나가 저장된 파일을 로드한다. DataFrame 객체를 반환한다. 

    ```python
    from pyspark.sql import SQLContext
    import sys

    sqlContext = SQLContext(sc)
    ghLog = sqlContext.read.json("2015-03-01-0.json")
    ghLog.printSchema()
    ghLog.show(5)

    ghLog.count() #17786
    pushes = ghLog.filter("type = 'PushEvent'")
    pushes.count()  #8793
    ```
    <img width="1000" alt="Screen Shot 2021-06-16 at 7 35 14 PM" src="https://user-images.githubusercontent.com/43725183/122204352-fc3ccc00-ced9-11eb-8404-29866a84399e.png">

3. 데이터 집계: `count`외에도 `min`, `max`, `avg`, `sum` 등의 집계 함수를 제공한다. 

    ```python
    grouped = pushes.groupBy("actor.login").count()
    ordered = grouped.orderBy(grouped['count'], ascending=False)
    ```

4. 분석대상 제외 & 공유변수

    : `broadcast`를 이용해서 **공유변수**를 만들자. 공유변수를 설정하지 않고 이대로 예제 프로그램을 실행하면 스파크는 `employees`를 대략 200회(필터링 작업을 수행할 태스크 개수) 가까이 반복적으로 네트워크에 전송하게 될 것이다. 

    반면 공유변수는 클러스터의 각 노드에 정확히 한 번만 전송한다. 또 공유변수는 클러스터 노드의 메모리에 자동으로 캐시되므로 프로그램 실행 중 바로 접근할 수 있다. 

    ```python
    employees = [line.rstrip('\n') for line in open(sys.argv[2])]
    bcEmployees = sc.broadcast(employees)

    def isEmp(user):
    	return user in bcEmployees.value

    #filter에 사용하기 위해서 user-defined function 등록
    from pyspark.sql.types import BooleanType
    sqlContext.udf.register("SetContainsUdf", isEmp, returnType=BooleanType())
    filtered = ordered.filter("SetContainsUdf(login)")
    ```

5. 애플리케이션 실행

    ```bash
    $ python main.py \
    	../github-archive/2015-03-01-0.json \   #	../github-archive/*.json \
    	../ch03/ghEmployees.txt
    ```
    <img width="200" alt="Screen Shot 2021-06-16 at 7 37 01 PM" src="https://user-images.githubusercontent.com/43725183/122204609-46be4880-ceda-11eb-8628-b179a7dedcfa.png">

    ```python
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SQLContext
    import sys

    if __name__ == "__main__":
    	sc = SparkContext(conf=SparkConf())
    	sqlContext = SQLContext(sc)
    	ghLog = sqlContext.read.json(sys.argv[1])

    	pushes = ghLog.filter("type = 'PushEvent'")
    	grouped = pushes.groupBy("actor.login").count()
    	ordered = grouped.orderBy(grouped['count'], ascending=False)

    	# Broadcast the employees set	
    	employees = [line.rstrip('\n') for line in open(sys.argv[2])]

    	bcEmployees = sc.broadcast(employees)

    	def isEmp(user):
    		return user in bcEmployees.value

    	from pyspark.sql.types import BooleanType
    	sqlContext.udf.register("SetContainsUdf", isEmp, returnType=BooleanType())
    	filtered = ordered.filter("SetContainsUdf(login)")
    	filtered.show()

    	filtered.write.format(sys.argv[4]).save(sys.argv[3])
       #파일 형식에는 JSON, Parquet, JDBC등이 가능하다. 
       #세번째 인수는 저장 경로를 적어준다. 
    ```

  </details>
  <details close>
  <summary>3.3. Submitting the application</summary><br>

스파크 프로그램을 운영 환경에서 원활하게 실행할 수 있도록 프로그램에 JAR 파일을 추가하는 방법은 (1) `spark-submit` 스크립트의 `—jars` 매개변수에 프로그램에 필요한 JAR 파일을 모두 나열해 실행자로 전송하거나, (2) 모든 의존 라이브러리를 포함하는 `underjar`를 빌드하는 방법이다. 

- uderjar 빌드: 일단은 생략
- `spark-submit`: 스파크 애플리케이션을 제출하는 일종의 헬퍼 스크립트로 애플리케이션을 스파크 클러스터에서 실행하는데 사용한다.

    ```bash
    ./bin/spark-submit \
      --class <main-class> \   #python에서는 사용 x
      --master <master-url> \
      --deploy-mode <deploy-mode> \
      --conf <key>=<value> \
      ... # other options
      <application-jar> \
      [application-arguments]
    ```

    결과 폴더에 있는 `SUCCESS` 파일은 작업을 성공적으로 완료했음을 의미한다. 각 `crc` 파일은 CRC(Cyclic Redundancy Check)코드를 계산해 각 데이터 파일의 유효성을 검사하는데 사용한다. `._SUCCESS.crc` 파일은 모든 CRC파일을 검사한 결과가 성공적이라는 것을 의미한다.  

    ```bash
    $ spark-submit \
    	main.py \
    	"../github-archive/2015-03-01-0.json" \ 
    	"../ch03/ghEmployees.txt" \
    	"emp-gh-push-output" "json"
    ```
        
    <img width="1000" alt="Screen Shot 2021-06-17 at 12 08 30 PM" src="https://user-images.githubusercontent.com/43725183/122324988-bc6ef680-cf64-11eb-946b-4ede59f690a4.png">

  </details></blockquote>
</details>
  
<details close>
<summary><b>chapter 4</b> The spark api in depth</summary>
  <br><blockquote>
  <details close>
  <summary>4.1. Working with pair RDDs</summary><br>
    
키-값 쌍은 전통적으로 연관배열(associative array)이라는 자료구조를 사용해 표현한다. 이 자료구조를 파이썬에서는 dictionary라고 하며, 스칼라와 자바에서는 map이라고 한다. 스파크에서는 키-값 쌍으로 구성된 RDD를 **pair RDD**라고 한다. 스파크의 데이터를 반드시 키-값 쌍 형태로 구성할 필요는 없지만, 실무에서는 Pair RDD를 사용하는 것이 바람직한 경우가 많다. 

- **Pair RDD 생성**: RDD의 keyBy 변환 연산자는 RDD 요소로 키를 생성하는 f 함수를 받고, 각 요소를 (f(요소), 요소)( = **(키, 요소)**) 쌍의 튜플로 매핑한다. 물론 데이터를 2-element 튜플로 직접 변환할 수도 있다. 어떤 방법이든 2-element 튜플로 RDD를 구성하면 **Pair RDD 함수**가 RDD에 자동으로 추가된다.

    ```python
    tranFile = sc.textFile("ch04/ch04_data_transactions.txt")
    tranData = tranFile.map(lambda line: line.split("#"))
    #예제 파일의 각 줄에는 구매 날짜, 시간, 고객 ID, 상품 ID, 구매수량, 구매 금액이 '#'로 구분되어있다. 
    ```
    <img width="400" alt="Screen Shot 2021-06-18 at 4 42 12 PM" src="https://user-images.githubusercontent.com/43725183/122525386-24503a80-d054-11eb-996d-d9a75f962093.png">

    ```python
    #키, 요소 페어 생성
    transByCust = tranData.map(lambda t: (int(t[2]), t))

    transByCust.keys().distinct().count() #100
    transByCust.count() #1000
    #map 함수로 키와 값을 만들어보자. 고객이 총 100명 있다.
    ```

- **Pair RDD 함수**
    - 키별 개수 세기(`CountByKey`): 각 키의 출현 횟수를 담은 Map 객체를 반환한다. ID가 53인 고객이 19번으로 가장 많이 구매했다.

        ```python
        transByCust.countByKey()
        cid, purch = sorted(transByCust.countByKey().items(), key= lambda x:-x[1])[0] ##(53, 19)

        #사은품 리스트 추가(가격 = 0)
        complTrans = [["2015-03-30", "11:59 PM", "53", "4", "1", "0.00"]]
        ```

    - 단일 키로 값 찾기: `transByCust.lookup(53)` → 53번 고객의 모든 구매 기록을 가져올 수 있다. lookup은 결과 값을 드라이버로 전송하므로 이를 메모리에 적재할 수 있는지 먼저 확인해야 한다.
        
        <img width="300" alt="Screen Shot 2021-06-18 at 4 43 00 PM" src="https://user-images.githubusercontent.com/43725183/122525491-3f22af00-d054-11eb-898a-df7f06895a28.png">

 

    - `mapValues` 변환 연산자로 Pair RDD 값 바꾸기: 키를 그대로 두고 값만 변경할 수 있다. 25번 상품을 2번 이상 구매한 경우 5% 할인해주자.

        ```python
        def applyDiscount(tran):
            if(int(tran[3])==25 and float(tran[4])>1):
                tran[5] = str(float(tran[5])*0.95)
            return tran

        transByCust = transByCust.mapValues(lambda t: applyDiscount(t))
        ```

    - `flapMapValues` 변환 연산자로 키에 값 추가: 81번 다섯 개 이상 구매한 고객에게 사은품으로 70번을 보내야한다. 즉, 해당 고객이 70번 상품을 구매한 것 처럼 `transByCust`에 추가해야한다.

        ```python
        def addToothbrush(tran):
            if(int(tran[3]) == 81 and int(tran[4])>4):
                from copy import copy
                cloned = copy(tran)
                cloned[5] = "0.00"
                cloned[3] = "70"
                cloned[4] = "1"
                return [tran, cloned]
            else:
                return [tran]

        transByCust = transByCust.flatMapValues(lambda t: addToothbrush(t))
        transByCust.count()  #1006 -> 6명이 81번 상품을 5개 이상 구매했다.
        ```

    - `reduceByKey` 또는 `foldByKey` 변환 연산자로 키의 모든 값 병합하기: reduceByKey는 각 키의 모든 값을 동일한 타입의 단일 값으로 변환한다. 이 연산자에는 두 값을 하나로 병합하는 Merge 함수를 전달해야하며, reduceByKey는 각 키별로 값 하나만 남을 때까지 merge 함수를 계속 호출한다. 따라서 merge 함수는 결합법칙을 만족해야한다.

        ```python
        amounts = transByCust.mapValues(lambda t: float(t[5])) #각 키별로 값만 뽑기 
        totals = amounts.foldByKey(0, lambda p1, p2: p1 + p2).collect()
        sorted(totals, key = lambda x:x[1])[-1] #(76, 100049.0)

        #사은품 리스트 추가(가격 = 0)
        complTrans += [["2015-03-30", "11:59 PM", "76", "63", "1", "0.00"]]
        #76번 고객이 10만 49달러로 가장 많이 썼다. 
        ```

        foldByKey는 reduceByKey와 기능은 같지만, merge 함수의 인자 목록 바로 앞에 zeroValue 인자를 담은 또 다른 인자 목록을 추가로 전달해야 한다는 점은 다르다. zeroValue는 반드시 항등원이어야한다. (e.g. 덧셈에서는 0, 곱셈에서는 1)

    - `union`으로 array 추가하기 + 결과 파일 저장하기

        ```python
        transByCust = transByCust.union(sc.parallelize(complTrans).map(lambda t: (int(t[2]), t)))
        transByCust.map(lambda t: "#".join(t[1])).saveAsTextFile("ch04/ch04output-transByCust")
        ```

    - `aggregateByKey`로 키의 모든 값 그루핑: aggregateByKey는 (1) **zeroValue**와 (2) 임의의 V 타입을 가진 값을 또 다른 U 타입으로 변환하는 **변환 함수(Transform)**, (3)변환함수가 변환한 값을 두 개씩 하나로 병합하는 **병합함수(Merge)**가 필요하다.

        ```python
        prods = transByCust.aggregateByKey([], 
        		lambda prods, tran: prods + [tran[3]],    #변환함수 -> 각 파티션 별로 요소를 병합
            lambda prods1, prods2: prods1 + prods2)   #병합함수 -> 최종 결과를 병합
        prods.collect()
        #prods에 빈 리스트가 전달되었다.
        ```
        <img width="1000" alt="Screen Shot 2021-06-18 at 4 43 31 PM" src="https://user-images.githubusercontent.com/43725183/122525565-52357f00-d054-11eb-8eb7-b7f6cba80cd0.png">

  </details>
  <details close>
  <summary>4.2. Understanding data partitioning and reducing data shuffling</summary>
    
* **데이터 파티셔닝(data partitioning)** : 데이터를 여러 클러스터 노드로 분할하는 메커니즘을 의미한다. 이 장에서는 일단 스파크 클러스터를 '병렬 연산이 가능하고 네트워크로 연결된 머신(즉, 노드)의 집합'정도로 생각하자.
    - **파티션(partition)**: 과거에는 파티션 대신 스플릿(split)용어를 사용했다. RDD의 파티션은 RDD 데이터의 일부(조각 또는 슬라이스)를 의미한다. 예를 들어 로컬 파일 시스템에 저장된 텍스트 파일을 스파크에 로드하면, 스파크는 파일 내용을 여러 파티션으로 분할해 클러스터 노드에 고르게 분산 저장한다. 여러 파티션을 노드 하나에 저장할 수도 있다. 이렇게 분산된 파티션이 모여서 RDD 하나를 형성한다.
    - 파티션의 개수: 해당 RDD에 변환 연산을 실행할 "태스크 개수"와 직결되므로 파티션의 개수는 매우 중요하다. 태스크 개수가 필요 이하로 적으면 클러스터를 충분히 활용할 수 없다. 게다가 각 태스크가 처리할 데이터 분량이 실행자의 메모리 리소스를 초과해 메모리 문제가 발생할 수 있다. 따라서 클러스터의 코어 개수보다 서너 배 더 많은 파티션을 사용하는 것이 좋다.
    - **데이터 partitioner**: RDD의 데이터 파티셔닝은 RDD의 각 요소에 파티션 번호를 할당하는 partitioner 객체가 수행한다. partitioner는 HashPartitioner, RangePartitioner, 또는 사용자 정의 Partitioner(Pair RDD의 경우)로 구현할 수 있다.
        - **HashPartitioner**: 스파크의 기본 Partitioner. HashPartitioner는 각 요소의 자바 해시코드를 단순한 mod 공식(partitionIndex = hashCode % numberOfPartitions)에 대입해 파티션 번호를 계산한다. 각 요소의 파티션 번호를 거의 무작위로 결정하기 때문에 모든 파티션을 정확하게 같은 크기로 분할할 가능성이 낮다. 하지만 대규모 데이터셋을 상대적으로 적은 수의 파티션으로 나누면 대체로 데이터를 고르게 분산시킬 수 있다. 
        (파티션의 기본 개수는 spark.default.parallelism 환경 매개변수 값으로 결정된다)
        - **RangePartitioner**: 정렬된 RDD의 데이터를 거의 같은 범위 간격으로 분할할 수 있다.
        - **Pair RDD의 사용자 정의 Partitioner**: 파티션의 데이터를 특정 기준에 따라 정확하게 배치해야 할 경우 사용자 정의 partitioner로 pair RDD를 분할할 수 있다. 
        (cf. in Python there is no version of aggregateByKey with a custom partitioner)
    - RDD 파티션 변경
        - `partitionBy`: PairRDD에서만 사용가능하고, 또 파티셔닝에 사용할 Partitioner 객체만 인자로 전달할 수 있다
        - `coalesce`와 `repartition`: coalesce는 파티션의 수를 줄이거나 늘리는데 사용한다. 파티션 개수를 늘리려면 shuffle 인자를 true로 설정해야한다. 반면 파티션 수를 줄일 때는 이 인자를 false로 설정할 수 있다. 이때는 새로운 파티션 개수와 동일한 개수의 부모 RDD 파티션을 선정하고 나머지 파티션의 요소를 나누어 선정한 파티션과 병합(coalesce)하는 방식으로 파티션 개수를 줄인다. 즉, 셔플링을 수행하지 않는 대신 데이터 이동을 최소화하려고 부모 RDD의 기존 파티션을 최대한 보존한다. 
        cf. repartition 변환 연산자는 단순히 shuffle을 true로 설정해 coalesce를 호출한 결과를 반환한다.
        - `repartitionAndSortWithinPartition`: 정렬 가능한 RDD에서만 사용할 수 있다. 새로운 Partitioner 객체를 받아 각 파티션 내에서 요소를 정렬한다. 이 연산자는 셔플링 단계에서 정렬 작업을 함께 수행하기 때문에 repartition을 호출한 후 직접 정렬하는 것보다 성능이 더 낫다.
- **데이터 셔플링(data shuffling)**: **파티션 간의 물리적인 데이터 이동을 의미한다.** 셔플링은 새로운 RDD의 파티션을 만들려고 여러 파티션의 데이터를 합칠 때 발생한다. 예를 들어 키를 기준으로 요소를 그루핑하려면 스파크는 RDD의 파티션을 모두 살펴보고 키가 같은 요소를 전부 찾은 후, 이를 물리적으로 묶어서 새로운 파티션을 구성하는 과정을 수행해야한다.

    ```python
    prods = transByCust.aggregateByKey([], 
    		lambda prods, tran: prods + [tran[3]],    #변환함수(Transform) -> 각 파티션 별로 요소를 병합
        lambda prods1, prods2: prods1 + prods2)   #병합함수(Merge) -> 최종 결과를 병합
    prods.collect()
    #4.1절의 aggregateByKey 예시를 다시 보자. 
    ```
    <img width="600" alt="Screen Shot 2021-06-18 at 4 45 57 PM" src="https://user-images.githubusercontent.com/43725183/122525886-a93b5400-d054-11eb-9dcd-ea556f230a4d.png">


    - 변환함수(Transform)은 각 파티션 별로 각 키의 값을 모아서 리스트를 구성한다.(**map task**) →  스파크는 이 리스트들을 각 노드의 중간 파일(interm files)에 기록한다 → 병합함수(merge)를 호출해 여러 파티션에 저장된 리스트들을 각 키별 단일 리스트로 병합한다.(**reduce task**) → 기본 partitioner(hash partitioner)를 적용해 각 키를 적절한 파티션에 할당한다.
    - 셔플링 바로 전에 수행한 태스크를 **맵(map) 태스크**라고 하며, 바로 다음에 수행한 태스크를 **리듀스(reduce)** 태스크라고 한다. 맵 태스크의 결과는 중간 파일에 기록하며(주로 운영체제의 파일 시스템 캐시에만 저장), 이후 리듀스 태스크가 이 파일을 읽어들인다. 중간 파일을 디스크에 기록하는 작업도 부담이지만, 결국 셔플링할 데이터를 네트워크로 전송해야 하기 때문에 스파크 잡의 셔플링 횟수를 최소한으로 줄이도록 노력해야 한다.
    - 셔플링 발생 조건
        1. partitioner를 명시적으로 변경하는 경우
            - 파티션 개수가 다른 HashPatitioner를 변환 연산자에 사용하거나, (`rdd.aggregateByKey(zeroValue, seqFunc, comboFunc, 100).collect()`)
            - 사용자 정의 Partitioner를 사용하면

            → 셔플링이 발생한다. 따라서 가급적이면 기본 partitioner를 사용해 의도하지 않은 셔플링은 최대한 피하는 것이 성능 면에서 가장 안전한 방법이다. 

        2. partitioner를 제거하는 경우

            변환 연산자에 partitioner를 명시적으로 지정하지 않았는데도 간혹 셔플링이 발생할 때가 있다. 대표적으로 map과 flatMap은 RDD의 Partitioner를 제거한다. 이 연산자 자체로는 셔플링이 발생하지 않지만, 연산자의 결과 RDD에 다른 변환 연산자를 사용하면 기본 Partitioner를 사용했더라도 여전히 셔플링이 발생한다.  → Pair RDD의 키를 변경하지 않는다면 map, flatMap 대신 mapValues, flatMapValues를 사용해 Partitioner를 보존하는 것이 좋다. 

            ```python
            rdd = sc.parallelize(range(10000))
            rdd.map(lambda x: (x, x*x)).map(lambda (x, y): (y, x)).collect() #셔플링 발생 x
            rdd.map(lambda x: (x, x*x)).reduceByKey(lambda v1, v2: v1+v2).collect() #셔플링 발생함
            ```

    - 셔플링을 수행하면 executor는 다른 executor(=실행자)의 파일을 읽어 들여야한다. 하지만 셔플링 도중 일부 실행자에 장애가 발생하면 해당 실행자가 처리한 데이터를 더 이상 가져올 수 없어서 데이터 흐름이 중단된다. → **외부 셔플링 서비스(external shuffling service)**는 실행자가 중간 셔플 파일을 읽을 수 있는 단일 지점을 제공해 셔플링의 데이터 교환 과정을 최적화할 수 있다. (`spark.shuffle.service.enabled=true`로 설정하기)
    - 셔플링 관련 매개변수
        - 셔플링 알고리즘 설정: `spark.shuffle.manager`의 값을 hash(해시 기반 셔플링), sort(정렬 기반 셔플링) → 정렬 기반 셔플링은 파일을 더 적게 생성하고 메모리를 더욱 효율적으로 사용할 수 있어 default 값이다.
        - 중간 파일의 통합 여부: `spark.shuffle.consolidateFiles` → ext4나 XFS 파일 시스템을 사용한다면 이 값을 true로 변경(default = false)로 하는 것이 좋다.
        - 셔플링에 쓸 메모리 리소스의 제한 여부: `spark.shuffle.spill` → 메모리를 제한하면 스파크는 제한 임계치를 초과한 데이터를 디스크로 내보낸다. 메모리 임계치를 너무 높게 설정하면 메모리 부족 예외(out-of-memory exception)이 발생할 수 있다. 반대로 너무 낮게 설정하면 데이터를 자주 내보내므로 균형을 잘 맞추는 것이 중요하다.
        - `spark.shuffle.compress`: 중간 파일의 압축 여부를 지정할 수 있다.
        - `spark.shuffle.spill.batchSize`: 데이터를 디스크로 내보낼 때 일괄로 직렬화 또는 역직렬화할 객체 개수를 지정한다.
        - `spark.shuffle.service.port`: 외부 셔플링 서비스를 활성화할 경우 서비스 서버가 사용할 포트 번호를 지정한다.
- 파티션 단위로 데이터 매핑: RDD의 각 파티션에 개별적으로 매핑 함수를 적용할수도 있다. 이 메서드를 잘 활용하면 각 파티션 내에서만 데이터가 매핑되도록 기존 변환 연산자를 최적화해 셔플링을 억제할 수 있다.
    - `mapPartitions`와 `mapPartitionsWithIndex`: mapPartitions는 각 파티션의 모든 요소를 반복문으로 처리하고 새로운 RDD 파티션을 생성한다. mapPartitionWithIndex는 매핑 함수에 파티션 번호가 함께 전달된다.
    - `glom`: 각 파티션의 모든 요소를 배열 하나로 모으고, 이 배열들을 요소로 포함하는 새로운 RDD를 반환한다. 따라서 새로운 RDD에 포함된 요소 개수는 이 RDD의 파티션 개수와 동일하다. glom 연산자는 기존의 Partitioner를 제거한다.

        ```python
        import random
        l = [random.randrange(100) for x in range(500)]
        rdd = sc.parallelize(l, 30).glom() #30개의 파티션으로 나눠져 있는 것을 모음. 
        rdd.collect()
        rdd.count() #30
        ```
  </details>
  <details close>
  <summary>4.3. Joining, sorting, and grouping data</summary>
    
* **데이터 조인(Join)** : 스파크에서는 RDBMS의 고전적인 join 연산자뿐만 아니라, zip, cartesian, intersection 등 다양한 변환 연산자를 사용해 여러 RDD 내용을 합칠 수 있다.

    <img width="621" alt="Screen Shot 2021-06-20 at 9 57 57 PM" src="https://user-images.githubusercontent.com/43725183/122675025-940c2400-d212-11eb-9db3-769ed31b6e25.png">

    RDD의 key를 product ID로 변경

    - RDBMS와 유사한 Join 연산자: `join`(RDBMS의 inner join과 동일), `leftOuterJoin`, `rightOuterJoin`, `fullOuterJoin` → 이 연산자들은 PairRDD에서만 사용할 수 있다. Partitioner 객체나 파티션 개수를 전달할 수 있다.

        ```python
        totalsByProd = transByProd.mapValues(lambda t: float(t[5])).reduceByKey(lambda tot1, tot2: tot1 + tot2)

        products = sc.textFile("first-edition/ch04/ch04_data_products.txt").map(lambda line: line.split("#")).map(lambda p: (int(p[0]), p))
        totalsAndProds = totalsByProd.join(products)
        totalsAndProds.first()
        #(68, (62133.899999999994, ['68', 'Niacin', '6295.48', '1']))
        #68은 key(productID), 그리고 value의 첫번째는 totalByProd의 값, 두번째 리스트는 products 정보.
        ```

        ```python
        totalsWithMissingProds = products.leftOuterJoin(totalsByProd)
        missingProds = totalsWithMissingProds.filter(lambda x: x[1][1] is None).map(lambda x: x[1][0])
        missingProds.foreach(lambda p: print(", ".join(p)))
        #3, Cute baby doll, battery, 1808.79, 2
        #43, Tomb Raider PC, 2718.14, 1
        #63, Pajamas, 8131.85, 3
        #20, LEGO Elves, 4589.79, 4
        #어제 판매하지 않은 상품 리스트 출력. 
        ```

    - `subtract`이나 `subtractByKey` 변환 연산자로 공통 값 제거: `substract`은 첫번째 RDD에서 두번째 RDD의 요소를 제거한 여집합을 반환한다. 이는 일반 RDD에서도 사용할 수 있으며, 각 요소의 키나 값만 비교하는 것이 아니라 요소 전체를 비교해 제거여부를 판단한다. 
    반면 `substracByKey`는 PairRDD에서만 사용할 수 있으며 키와 값을 모두 보는 것이 아니라 키만 본다.

        ```python
        missingProds = products.subtractByKey(totalsByProd)
        missingProds.foreach(lambda p: print(", ".join(p[1])))
        #leftOuterJoin후 None 값인 record만 뽑아내는 과정대신 substractByKey를 사용하면 된다. 
        ```

    - `cogroup` 변환 연산자로 RDD 조인: `cogroup`은 여러 RDD 값을 각각 키로 그루핑한 후 키를 기준으로 조인한다. `cogroup`은 RDD를 최대 세개까지 조인할 수 있는데, 단 `cogroup`을 호출한 RDD와 `cogroup`에 전달된 RDD는 모두 동일한 타입의 키를 가져야 한다.

        ```python
        prodTotCogroup = totalsByProd.cogroup(products)
        prodTotCogroup.first()
        #(68, (<pyspark.resultiterable.ResultIterable object at 0x107d3ab90>, <pyspark.resultiterable.ResultIterable object at 0x107da1fd0>))

        prodTotCogroup.filter(lambda x: len(x[1][0].data) == 0).foreach(lambda x: print(", ".join(x[1][1].data[0])))
        #total 가격과 product 정보의 outer join. 두 RDD 중 한쪽에만 등장한 키의 경우 다른 쪽 RDD iterator는 비어 있다. 
        ```

    - `intersection` 변환 연산자 사용: `intersection`은 교집합을 찾는다.

        ```python
        totalsByProd.map(lambda t: t[0]).intersection(products.map(lambda p: p[0]))
        ```

    - `cartesian` 변환 연산자로 RDD 두개 결합: 두 RDD의 데카르트 곱을 계산한다(요소의 모든 combination return). 따라서 이를 이용해 두 RDD 요소들을 서로 비교하는데도 사용할 수 있다.

        ```python
        rdd1 = sc.parallelize([7,8,9])
        rdd2 = sc.parallelize([1,2,3])
        rdd1.cartesian(rdd2).collect()
        #[(7, 1), (7, 2), (7, 3), (8, 1), (8, 2), (8, 3), (9, 1), (9, 2), (9, 3)]
        rdd1.cartesian(rdd2).filter(lambda el: el[0] % el[1] == 0).collect()
        #rdd2숫자로 rdd1 숫자를 나머지 없이 나눌 수 있는 조합 찾기
        [(7, 1), (8, 1), (8, 2), (9, 1), (9, 3)]
        ```

    - `zip` 변환 연산자로 RDD 조인: 모든 RDD에 사용가능. 첫번째 쌍은 각 RDD의 첫번째 요소를 조합한 것이며, 두번째 쌍은 두 번째 요소를 조합하고, 세 번째 쌍은 세 번째 요소를 조합하는 식으로 끝까지 이어진다. → 두 RDD의 파티션 개수가 다르거나 두 RDD의 모든 파티션이 서로 동일한 개수의 요소를 포함하지 않으면 오류를 발생한다.

        ```python
        rdd1 = sc.parallelize([1,2,3])
        rdd2 = sc.parallelize(["n4","n5","n6"])
        rdd1.zip(rdd2).collect()
        #[(1, 'n4'), (2, 'n5'), (3, 'n6')]
        ```

    - `zipPartitions` 변환 연산자로 RDD 조인: 모든 RDD에 사용가능. RDD는 서로 파티션 개수가 동일해야하지만, 모든 파티션이 서로 동일한 개수의 요소를 가져야 한다는 조건을 꼭 만족하지 않아도 된다. → 아직 python에서는 사용할 수 없다.
- **데이터 정렬(sorting)**
    - `repartitionAndSortWithinPartition`(4.2 절 참조), `sortByKey`, `sortBy`을 사용해서 정렬할 수 있다.

        ```python
        sortedProds = totalsAndProds.sortBy(lambda t: t[1][1][1])
        sortedProds.collect()
        ```

        이 결과는 `totalsAndProds` RDD에서 상품 이름을 키로 끄집어낸 후 `sortByKey`를 호출해서도 확인할 수 있다. 

    - 정렬 가능한 클래스 생성하기
    - 이차 정렬: 파티션을 2개로 바꾸고, 각 파티션 내에서 요소를 정렬한다.

        ```python
        rdd = sc.parallelize([(0, 5), (3, 8), (2, 6), (0, 8), (3, 8), (1, 3)])
        rdd2 = rdd.repartitionAndSortWithinPartitions(2)
        rdd2.glom().collect()
        #[[(0, 5), (0, 8), (2, 6)], [(1, 3), (3, 8), (3, 8)]]
        #파티션1: [(0, 5), (0, 8), (2, 6)]
        #파티션2: [(1, 3), (3, 8), (3, 8)]
        ```
        <img width="300" alt="Screen Shot 2021-06-20 at 9 59 42 PM" src="https://user-images.githubusercontent.com/43725183/122675077-d46ba200-d212-11eb-852c-ac15c055cf3c.png">


    - `top`과 `takeOrdered`로 정렬된 요소 가져오기: 이 두 메서드는 전체 데이터를 정렬하지 않는다. 그 대신 각 파티션에서 상위(또는 하위) n개 요소를 가져온 후 이 결과를 병합하고, 이 중 상위(또는 하위) n개 요소를 반환한다. → 두 메서드는 훨씬 더 적은 양의 데이터를 네트워크로 전송하며, sortBy와 take를 개별적으로 호출하는 것보다 무척 빠르다.
- **데이터 그루핑(grouping)**: 데이터 그루핑은 데이터를 특정 기준에 따라 단일 컬렉션으로 집계하는 연산을 의미한다.
    - `aggregateByKey`: 4.1.2절 참조
    - `groupBykey`(`groupBy`): groupByKey는 동일한 키를 가진 모든 요소를 단일 키-값 쌍으로 모은 Pair RDD를 반환한다. groupBy는 PairRDD가 아닌 일반 RDD에서도 사용할 수 있으며, 일반 RDD를 PairRDD로 변환하고 groupByKey를 호출하는 것과 같은 결과를 만들 수 있다. groupByKey는 각 키의 모든 값을 메모리로 가져오기 때문에 이 메서드를 사용할 때는 메모리 리소스를 과다하게 사용하지 않도록 주의해야 한다. 모든 값을 한꺼번에 그루핑할 필요가 없으면 aggregateByKey나 reduceByKey, foldByKey를 사용하는 것이 좋다.
    - `combineByKey`: combineByKey를 호출하려면 세 가지 커스텀 함수를 정의해 전달해야 한다. 첫번째 함수는 `createCombiner`로 각 파티션별로 키의 첫번째 값에서 최초 결합값을 생성하는 데 사용한다. 두번째 함수(`mergeValue`)는 Pair RDD에 저장된 값들을 결합값(combined value)로 병합하고(동일 파티션 내에서 해당 키의 다른 값을 결합 값에 추가해 병합하는데 사용), 세번째 함수(`mergeCombiner`)는 결합 값을 최종 결과를 병합(여러 파티션의 결합 값을 최종 결과로 병합)한다.

        ```python
        def createComb(t):
            total = float(t[5])   #구매 금액
            q = int(t[4])         #구매 수량
            return (total/q, total/q, q, total)  #상품 낱개의 가격을 계산해 최저가격과 최고 가격의 초깃값으로 사용.

        def mergeVal(p,t):
            mn,mx,c,tot = p
            total = float(t[5])
            q = int(t[4])
            return (min(mn,total/q),max(mx,total/q),c+q,tot+total)

        def mergeComb(p1, p2):
            mn1,mx1,c1,tot1 = p1
            mn2,mx2,c2,tot2 = p2
            return (min(mn1,mn1),max(mx1,mx2),c1+c2,tot1+tot2)

        avgByCust = transByCust.combineByKey(createComb, mergeVal, mergeComb).\
        mapValues(lambda t: (t[0], t[1], t[2], t[3], t[3]/t[2]))
        avgByCust.first()

        totalsAndProds.map(lambda p: p[1]).map(lambda x: ", ".join(x[1])+", "+str(x[0])).saveAsTextFile("ch04/ch04output-totalsPerProd")
        avgByCust.map(lambda (pid, (mn, mx, cnt, tot, avg)): "%d#%.2f#%.2f#%d#%.2f#%.2f" % (pid, mn, mx, cnt, tot, avg)).saveAsTextFile("ch04/ch04output-avgByCust")
        ```
  </details>
  <details close>
  <summary>4.4. Understanding RDD dependencies</summary>
    
* **RDD의 의존 관계(dependency)** : 스파크의 실행 모델은 **Directed Acyclic Graph(DAG)** 에 기반한다. 방향성(directed) 그래프는 간선이 한 정점에서 다른 정점을 가리키는 방향성이 있는 그래프를 의미한다. 그 중 DAG는 간선의 방향을 따라 이동했을 때 같은 정점에 두 번 이상 방문할 수 없도록 연결된 그래프를 의마한다. (이름 그대로 acyclic하다)
**스파크의 DAG는 RDD를 정점(vertex)로, RDD 의존관계를 간선(edge)로 정의한 그래프를 의미한다.** RDD의 변환 연산자를 호출할 때마다 새로운 정점(RDD)와 새로운 간선(의존관계)이 생성된다. 변환 연산자로 생성된 RDD가 이전 RDD에 의존하므로 간선방향은 자식 RDD(새 RDD)에서 부모 RDD(이전 RDD)로 향한다. 이러한 RDD 의존 관계 그래프를 **RDD lineage**라고 한다.
    - **좁은(narrow) 의존 관계**: 데이터를 다른 파티션으로 전송할 필요가 없는 변환 연산은 좁은 의존 관계를 형성한다.
        - one-to-one 의존관계: range 의존관계 이외의 모든 셔플링이 필요하지 않은 변환연산자로 생성.
        - range 의존관계: 여러 부모 RDD에 대한 의존 관계를 하나로 결합한 경우로 union 변환연산자만 해당.
    - **넓은(wide, 또는 shuffle) 의존 관계**: 셔플링을 수행할 때 형성된다. 참고로 RDD를 조인하면 셔플링이 항상 발생한다.<br><br>

    ```python
    import random
    l = [random.randrange(10) for x in range(500)]
    listrdd = sc.parallelize(l, 5)  #파티션 5개로 나누기
    pairs = listrdd.map(lambda x: (x, x*x))     

    reduced = pairs.reduceByKey(lambda v1, v2: v1+v2)

    finalrdd = reduced.mapPartitions(lambda itr: ["K="+str(k)+",V="+str(v) for (k,v) in itr])
    finalrdd.collect()
    print(finalrdd.toDebugString())
    #map은 좁은(one-to-one) 의존 관계를 만드는 연산자. reduceBykey는 넓은(셔플) 의존관계를 형성
    ```

    <img width="1000" alt="Screen Shot 2021-06-20 at 9 55 12 PM" src="https://user-images.githubusercontent.com/43725183/122674936-34157d80-d212-11eb-8c49-cca2da11a64f.png">



    ```
    //print(finalrdd.toDebugString())의 결과

    (5) PythonRDD[5] at collect at <stdin>:1 []
     |  MapPartitionsRDD[4] at mapPartitions at PythonRDD.scala:133 []
     |  ShuffledRDD[3] at partitionBy at NativeMethodAccessorImpl.java:0 []
     +-(5) PairwiseRDD[2] at reduceByKey at <stdin>:1 []
        |  PythonRDD[1] at reduceByKey at <stdin>:1 []
        |  ParallelCollectionRDD[0] at parallelize at PythonRDD.scala:195 []
    ```
    5번 RDD가 가장 최근에 만들어진 RDD. ShuffledRDD가 출력된 지점을 곧 셔플링을 수행하는 시점으로 간주할 수 있다. → 이러한 출력 결과를 잘 활용하면 스파크 프로그램의 셔플링 횟수를 최소화할 수 있다. 
    <br>괄호안 숫자(5)는 해당 RDD의 파티션 개수를 의미한다. 

- **스파크의 stage와 task**: 스파크는 셔플링이 발생하는 지점을 기준으로 스파크 잡(job) 하나를 여러 스테이지(stage)로 나눈다.
    <img width="1000" alt="Screen Shot 2021-06-20 at 9 54 13 PM" src="https://user-images.githubusercontent.com/43725183/122674901-0f210a80-d212-11eb-92c4-cdab59d039b1.png">


    - **1번 stage**는 셔플링으로 이어지는 parallelize, map, reduceByKey 변환 연산들이 포함된다. 1번 스테이지 결과는 중간 파일의 형태로 실행자 머신의 로컬 디스크에 저장된다. **2번 stage**에서는 이 중간 파일의 데이터를 적절한 파티션으로 읽어 들인 후 두 번째 map 변환 연산자부터 마지막 collect 연산까지 실행한다.
    - 스파크는 각 stage와 partition별로 task를 생성해 실행자에게 전달한다. 스테이지가 셔플링으로 끝나는 경우 이 단계의 태스트를 **shuffle-map task**(i.e. 1번 stage의 task)라고 한다. 스테이지의 모든 태스크가 완료되면 드라이버는 다음 스테이지의 태스크를 생성하고 실행자에 전달한다. 이 과정은 마지막 스테이지 결과를 드라이버로 반환할때까지 계속한다. 마지막 스테이지에 생성된 태스크를 **result task**(i.e. 2번 stage의 task)라고 한다.
- **RDD의 체크포인트(checkpoint)**: 변환 연산자를 계속 이어 붙이면 RDD lineage가 제약 없이 길어질 수 있다. 따라서 스파크는 중간에 스냅샷으로 RDD를 스토리지에 보관할 수 있는 **checkpointing** 방법을 제공한다. 일부 노드에 장애가 발생해도 유실된 RDD 조각을 처음부터 다시 계산할 필요가 없고, 그 대신 스냅샷 지점부터 나머지 lineage를 다시 계산한다.
    - 체크포인팅을 실행하면 스파크는 RDD의 데이터뿐만 아니라 RDD의 lineage까지 모두 저장한다. 체크포인팅을 완료한 후에는 저장한 RDD를 다시 계산할 필요가 없으므로 해당 RDD의 의존 관계와 부모 RDD 정보를 삭제한다.
    - 체크 포인팅은 RDD의 checkpoint 메서드를 호출해 실행할 수 있다. 하지만 먼저 SparkContext.setCheckpointDir에 데이터를 저장할 디렉터리부터 지정해야 한다. checkpoint 메서드는 해당 RDD에 job이 실행되기 전에 호출해야 하며, 그 이후 체크포인팅을 실제로 완료하려면 RDD에 행동 연산자(Action) 등을 호출해 잡을 실행하고 RDD를 구체화(materialize)해야한다.
  </details>
  <details close>
  <summary>4.5. Using accumulators and broadcast variables to communicate with Spark executors</summary><br>
    
* **누적 변수(accumulator)** 로 실행자에서 데이터 가져오기: 누적 변수는 여러 실행자가 공유하는 변수로 값을 더하는 연산만 허용한다. 누적 변수는 태스크의 진행상황을 추적하는데 사용할 수 있다.

    ```python
    #accumulators in Python cannot be named
    acc = sc.accumulator(0)  #0은 initial value
    l = sc.parallelize(range(1000000))
    l.foreach(lambda x: acc.add(1))
    acc.value  #누적 변수 값 가져오기. 이 값은 오직 드라이버만 참조할 수 있다. 
    # 1000000  -> 1을 1000000번 더한 결과

    #exception occurs(executor가 참조한 경우)
    l.foreach(lambda x: acc.value)
    #"Accumulator.value cannot be accessed inside tasks"

    #accumulables are not supported in Python
    #accumulableCollections are not supported in Python
    ```

- **공유 변수(broadcast variable)** 로 실행자에 데이터 전송: 공유변수는 여러 클러스터 노드가 공동으로 사용할 수 있는 변수다. 공유변수는 누적변수와 달리 실행자가 수정할 수 없다. 오직 드라이버만이 공유변수를 생성하며, 실행자에서는 읽기 연산만 가능하다.
    - **실행자 대다수가 대용량의 데이터를 공용으로 사용할 때는 이 데이터를 공유 변수로 만드는 것이 좋다.(핵심 point1)** 보통은 드라이버에서 생성한 변수를 태스크에서 사용하면 스파크는 이 변수를 직렬화하고 태스크와 함께 실행자로 전송한다. 하지만 드라이버 프로그램은 동일한 변수를 여러잡에 걸쳐 재사용할 수 있고 잡 하나를 수행할 때도 여러 태스크가 동일한 실행자에 할당될 수 있으므로, 변수를 필요 이상으로 여러번 직렬화해 네트워크로 전송하는 상황이 발생할 수 있다. 이때는 데이터를 더욱 최적화된 방식으로 단 한 번만 전송하는 공유 변수를 사용하는 편이 좋다. (3.2절 예시 참조)
    - 공유 변수는 Broadcast 타입의 객체를 반환하는 SparkContext.broadcast(value) 메서드로 생성한다. value 인수에는 직렬화 가능한 모든 종류의 객체를 전달할 수 있다.
    - **공유변수값을 참조할 때는 항상 value 메서드를 사용해야 한다.(핵심 point2)** 그렇지 않고 공유 변수에 직접 접근하면 스파크는 이 변수를 자동으로 직렬화해 태스크와 함께 전송한다. 이렇게 하면 공유 변수를 사용하는 이유, 즉 성능상 이득을 완전히 잃어버리게 된다.
    - 더 이상 필요하지 않은 공유변수는 destroy를 호출해 완전히 삭제(실행자와 드라이버에서 제거)할 수 있다. 또는 unpersist 메서드를 호출해 공유 변수 값을 실행자의 캐시에서 제거할 수 있다.
    - 공유변수와 관련된 스파크 매개변수
        - `spark.broadcast.compress`: 공유 변수를 전송하기 전에 데이터를 압축할지 여부를 지정한다.
        - `spark.broadcast.blockSize`: 공유 변수를 전송하는데 사용하는 데이터 청크의 크기를 설정한다. 실전 테스트를 거쳐 도출한 기본 값(4096KB)를 그대로 유지하면 좋다.
        - `spark.python.worker.reuse`: 파이썬의 공유 변수 성능에 큰 영향을 주는 매개변수다. 워커를 재사용하지 않으면 각 태스크별로 공유 변수를 전송해야 한다. 기본값이 true를 유지하면 좋다.
  </details></blockquote>
</details>

## 2장 meet the spark family
<details close>
  <summary><b>chapter 5</b> sparkling queries with spark sql</summary>
</details>  
<details close>
  <summary><b>chapter 6</b> ingesting data with spark streaming</summary>
</details>  
<details close>
  <summary><b>chapter 7</b> getting smart with MLlib</summary><br>
  <blockquote>
    <details close>
    <summary>7.1 Introduction to machine learning</summary>
    </details>
    <details close>
    <summary>7.2. Linear algebra in Spark</summary>
    </details>
    <details close>
    <summary>7.3. Linear regression</summary>
    </details>
    <details close>
    <summary>7.4. Analyzing and preparing the data</summary>
    </details>
    <details close>
    <summary>7.5. Fitting and using a linear regression model</summary>
    </details>
    <details close>
    <summary>7.6. Tweaking the algorithm</summary>
    </details>
    <details close>
    <summary>7.7. Optimizing linear regression</summary>
    </details>
  </blockquote>
</details>  
<details close>
  <summary><b>chapter 8</b> ML: classification and clustering</summary>
</details>  
<details close>
  <summary><b>chapter 9</b> connecting the dots with graphX</summary>
</details>  


## 3장 spark ops

<details close>
  <summary><b>chapter 10</b> Running spark</summary><br>
    <blockquote>
    <details close>
    <summary>10.1 An overview of Spark’s runtime architecture</summary>
      
* 스파크 런타임 컴포넌트: Spark는 실행되는 동안 다음 세 종류의 process를 생성한다. 아래 세 process는 Spark runtime architecture를 구성하는 **component(컴포넌트)** 들이다.<br>
    <img width="623" alt="Screen Shot 2021-06-21 at 6 41 04 PM" src="https://user-images.githubusercontent.com/43725183/122741775-43eb9b00-d2c0-11eb-958e-60b43ef70266.png">

    Driver의 위치는 클러스터 유형과 설정에 따라 다르다. 

    1. **client-process(클라이언트)**: driver process를 생성하는 프로세스로 spark 동작을 위한 classpath, configuration option 뿐만 아니라 application 변수를 포함한다. spark-submit script, spark-shell script 또는 custom application의 형태로 제공된다.  
    2. **driver(드라이버)**: Spark application에는 반드시 한 개의 driver process가 필요하다. driver process는 SparkContext와 Scheduler를 포함하며, 전체 spark application의 execution을 총괄하는 main function과 같은 존재이다.
        - driver process와 그 하위 컴포넌트(sparkcontext, scheduler)는 
        (1) cluster manager에게 memory와 CPU resource를 요청한다.
        (2) application logic을 stage와 task 단위로 조각낸다.
        (3) 각 task를 executor들에게 나눠준다.
        (4) 각 executor의 결과를 모은다.
        - deploy mode에 따른 분류 : driver process가 client JVM에 포함되면 **client-deploy mode**라고 부르고, client JVM과 분리되어 cluster내의 하나의 JVM으로 따로 구현되면 **cluster-deploy mode**라 부른다.
        - driver process가 시작될 때 특정 application의 configuration이 반영된 `SparkContext` instance가 생성된다. 스파크 REPL 셸은 셸 자체가 드라이버 프로그램 역할을 하며, 스파크 컨텍스트를 미리 설정해 `sc`라는 변수로 제공한다. JAR파일을 제출하거나 또 다른 프로그램에서 스파크 API로 스파크 독립형 애플리케이션을 실행할 때는 애플리케이션에서 직접 스파크 컨텍스트를 생성해야 한다.  JVM당 하나의 `SparkContext`가 필요하다(여러개의 SparkContext instance를 생성할 수도 있지만, 사용하지 말자). 
        → 스파크 컨텍스트는 RDD를 생성하거나 데이터를 로드하는 등 다양한 작업을 수행하는 여러 유용한 메서드를 제공하며, 스파크 런타임 인스턴스에 접근할 수 있는 기본 인터페이스이다.
    3. **executor(실행자)**: driver로부터 받은 task를 수행하고, 그 결과를 다시 driver로 보내는 역할을 한다. executor는 여러 개의 task slot을 가지고 있는데, 이는 thread로 구현되어 병렬 작업을 가능하게 한다. 일반적으로 CPU core 수의 2,3배 값을 task slot으로 설정한다. task slot은 스레드로 구현되므로 머신의 물리적인 CPU 코어 개수와 반드시 일치할 필요는 없다. 
- 스파크 클러스터 유형
    - Spark on clusters
        - **Standalone cluster**: 스파크 자체 클러스터. 스파크 자체 클러스터는 스파크 전용 클러스터다. 이 클러스터는 오직 스파크 애플리케이션에만 적합하도록 설계되었기 때문에 kerberos 인증 프로토콜로 보호된 HDFS를 지원하지 않는다. 이러한 보안 기능이 필요할 경우에는 YARN을 사용해 스파크를 실행해야 한다. 반면 스파크 자체 클러스터는 잡 시작에 걸리는 시간이 YARN보다 더 짧다.
        - **YARN cluster**: 하둡의 리소스 매니저 및 작업 실행 시스템이다. 하둡 버전 1의 맵리듀스 엔진을 대체한 것으로 맵리듀스 버전2라고도 한다.
        - **Mesos cluster**: 메소스는 확장성과 장애 내성을 갖춘 c++기반 분산 시스템 커널이다.
    - **Spark local modes**
    : special cases of a Spark "standalone cluster" running on "**a single machine**"
        - Spark local mode
        - Spark local cluster mode
    </details>
    <details close>
    <summary>10.2. Job and resource scheduling</summary>
    </details>
    <details close>
    <summary>10.3. Configuring Spark</summary>
    </details>
    <details close>
    <summary>10.4. Spark web UI</summary>
    </details>
    <details close>
    <summary>10.5. Running Spark on the local machine</summary>
    </details>
  </blockquote>
</details>  
<details close>
  <summary><b>chapter 11</b> Running on a spark standalone cluster</summary>
</details>  
<details close>
  <summary><b>chapter 12</b> Running on YARN and Mesos</summary><br>
</details>  

## 4장 Bringing it together

### Appendix
<details close>
<summary><b>A. Installing Apache Spark</b></summary>
  
1. 자바(JDK) 설치
    - 자바가 설치되어 있는지 확인하기: `which javac`
    (이 명령은 javac 명령을 실행했을 때 실제로 호출될 실행 파일 위치를 반환한다.)
    - (만약 자바가 설치되어 있다면)JAVA_HOME 환경 변수가 올바르게 설정되어 있는지 확인해본다.

        ```bash
        #환경 변수 설정되어 있는지 확인
        $echo $JAVA_HOME #open-jdk 설치 폴더

        #환경 변수 설정
        $echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" | sudo tee /etc/profile.d/sia.sh
        $source /etc/profile.d/sia.sh 
        ```

    - (만약 자바가 설치되어 있지 않다면)JDK 설치하기:  `sudo apt-get -y install openjdk-8-jdk`
2. 아파치 스파크 내려받고 설치 및 설정
    - 스파크 내려받기 페이지([http://spark.apache.org/downloads.html](http://spark.apache.org/downloads.html))에서 <br>
    (1) 최신 스파크 릴리스 선택(Choose a Spark release),<br>
    (2) 최신 하둡 버전으로 빌드된 패키지 유형 선택(Choose a package type),<br>
    (3) 스파크 다운로드(Download Spark)<br>
    아파치 미러의 목록이 나오면 가장 상위에 추천된 미러 사이트를 클릭한다.
    - 압축 풀고 설치: 리눅스의 표준 규칙에 따라 사용자 홈 디렉토리 아래의 bin 디렉토리를 스파크 바이너리 위치로 사용한다. 홈 디렉토리에는 전체 권한(읽기, 쓰기, 실행)이 부여되므로 매번 sudo를 입력할 필요가 없다.

        ```bash
        $cd $HOME/Downloads
        $tar -xvf spark*
        $rm spark*tgz

        ##홈디렉토리 아래의 bin 디렉토리로 스파크 바이너리 옮기기
        $cd $HOME
        $mkdir -p ~/bin/sparks
        $mv Downloads/spark-* bin/sparks
        ```

    - 여러 버전의 스파크를 사용할 경우 bin 디렉토리 아래에 sparks 디렉토리를 생성하고 이 안에 스파크의 현재 및 미래 버전을 모두 저장하자. 그리고 현재 사용할 버전에 심볼릭 링크를 만들자.

        ```bash
        $cd $HOME/bin
        $ln -s sparks/spark-2.0.0-bin-hadoop2.7 spark
        $tree -L 2 #심볼릭 링크 확인. 여기서 spark 폴더는 sparks 폴더 내의 다른 폴더를 가리키는 심볼릭 링크를 가리킨다. 

        #심볼릭 링크 바꾸기(기존의 심볼릭 링크 지우고, 새로 심볼릭 링크 만들기)
        $rm spark
        $ln -s spark-1.6.1-bin-hadoop2.6 spark
        ```

3. 스파크 셸
    - 스파크 셸 시작하기

        ```bash
        $cd $HOME/bin/spark
        $./bin/spark-shell 
        ```

    - 로깅레벨 변경하기: 스파크의 LOG4J 설정을 변경해 스파크 셸에는 오류 로그만 출력하고, 나머지 로그는 추후 문제 진단에 사용할 수 있도록 스파크 루트 폴더 아래의 logs/info.log 파일에 저장해보자.

        ```bash
        $gedit conf/log4j.properties  #스파크의 conf 디렉터리 아래에 log4j.properites 파일을 생성. log4j.properties 파일은 ch2 참조
        ```
    - 스파크 REPL(=스파크 셸)은 스파크 컨텍스트와 SparkSession 객체를 각각 sc와 spark라는 변수로 제공한다. 스파크 애플리케이션은 SparkSession을 통해 스파크에 접속하고 세션 설정, 작업 실행 관리, 파일 읽기 및 쓰기 작업 등을 수행할 수 있다. 스파크 REPL에 spark.까지만 입력하고 tab을 누르면 SparkSession이 제공하는 모든 함수를 볼 수 있다.
    - 파이썬 셸로 spark를 시작하는 경우: `$pyspark`
</details>
<details close>
  <summary><b>B. Understanding MapReduce</b></summary>
(MapReduce: Simplified Data Processing on Large Clusters)

- Job을 잘게 분할하고 클러스터의 모든 노드로 **map**ping, 즉 파견해 분산 처리를 수행하는 새로운 패러다임. 각 노드는 분할된 job을 처리한 중간 결과를 생성한다. 그 다음 분할된 중간 결과를 **reduce**, 즉 집계해 최종 결과를 낸다.
    - 맵리듀스는 최소한의 API만 노출해 대규모 분산 시스템을 다루는 복잡한 작업을 효과적으로 감추었다. map과 reduce만 노출함.
    - 맵리듀스는 데이터를 굳이 옮겨서 처리할 필요가 없다. 대신 데이터가 저장된 곳으로 "프로그램"을 전송한다.
- **Word count 문제**(빅데이터 분야의 hello world같은 존재)
한 웹 사이트에서 흔히 사용하는 단어를 분석해달라.

    ```
    Is it easy to program with MapReduce?
    Is it as easy as Map and Reduce are.
    ```

    1. `map`: 각 문장을 읽고 단어로 분할한다. 그다음 대문자를 소문자로 변환하고, 구두점을 제거한 후 각 단어마다 1이라는 값을 생성한다. 즉, key가 단어이고 value가 count(=1)이다. 
    → map은 입력된 각 문장별로 키와 값을 받아 키-값 쌍의 목록을 반환한다. 
    2. **shuffle phase:** 동일한 단어를 동일한 리듀서에 전달한다. 거의 모든 애플리케이션에서 가장 좁은 병목으로 작용한다. 
    3. `reduce`: map 함수의 return값을 받고, 단어별 출현 횟수를 합산한 후 최종 결과를 생성한다. 
- (정리) map 함수는 키-값 쌍을 가져와 임의의 변환 연산을 적용하고 중간 결과물로 키-값 쌍의 목록을 반환한다. 그런 다음 맵리듀스 API는 map 함수가 반환한 키-값 쌍을 키별로 그루핑해 reduce 함수의 입력으로 전달한다. reduce 함수는 입력된 값들을 임의의 방법으로 병합하고 그 결과를 최종 출력파일에 쓴다. 모든 리듀서가 작업을 완료하면 사용자 프로그램으로 제어를 반환한다.
</details>
<details close>
  <summary><b>C. A primer on linear algebra</b></summary><br>
  
- 행렬과 벡터
- 행렬 덧셈
- 스칼라배
- 행렬곱셈
- 단위행렬
- 역행렬: 역행렬을 갖는 정사각 행렬을 non-singular matrix라고 한다.
</details>
