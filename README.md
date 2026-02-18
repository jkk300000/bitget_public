# 비트겟 실시간 자동매매 시스템

비트겟(Bitget) 거래소의 비트코인 현물/선물 실시간 자동매매 시스템입니다.
실제 운용 중인 전략이라 일부 코드를 제외하고 업로드 했습니다.

## 📌 주요 기능

### 🔄 실시간 데이터 처리
- **WebSocket 실시간 데이터 수신**: 1분봉 캔들, 거래 체결 데이터
- **자동 데이터 복구**: 연결 끊김 시 과거 데이터 자동 복구

### 📊 기술적 지표
- **ATR (Average True Range)**: 변동성 측정
- **Squeeze Momentum**: 볼린저 밴드 + 켈트너 채널 기반 모멘텀
- **Support/Resistance**: 자동 지지/저항 레벨 계산

### 💰 전략 기능
- **복리 투자**: 수익 자동 재투자
- **부분 익절**: 단계별 익절 시스템
- **실시간 리스크 관리**: 포지션, 청산가 실시간 모니터링

### 🔐 계정 통합
- **실시간 계정 동기화**: 실제 포지션 정보 자동 동기화
- **잔고 관리**: USDT 잔고 실시간 추적
- **포지션 추적**: 롱/숏 포지션 자동 감지 및 관리



## 📁 프로젝트 구조

```
bitget/
├── main.py                          # 메인 실행 파일
├── bitget_client.py                 # WebSocket 클라이언트
├── bitget_account.py                # 계정 관리 (CCXT)
├── data_manager.py                  # 데이터 관리 및 리샘플링
├── strategy.py                      # 거래 전략
├── risk_manager.py                  # 리스크 관리
├── indicators.py                    # 기술적 지표 계산
├── data_recovery.py                 # 데이터 복구
├── check_api_keys.py                # API 키 테스트
├── requirements.txt                 # Python 패키지 의존성
├── .env                             # 환경 변수 (Git 제외)
├── cloud-init-userdata.yml          # Vultr Cloud-Init 스크립트
├── setup-cloud-init.sh              # Cloud-Init 자동 설정 스크립트
└──README.md                        # 이 파일

```

## 🔧 시스템 요구사항

### 로컬 실행
- **Python**: 3.9 이상
- **OS**: Windows, macOS, Linux
- **메모리**: 최소 2GB RAM
- **네트워크**: 안정적인 인터넷 연결

### 클라우드 배포 (Vultr)
- **OS**: Ubuntu 24.04 LTS x64
- **권장 사양**: 2GB RAM, 1 vCPU, 50GB SSD
- **비용**: 약 $12/월

---

## 📦 의존성 패키지

```
pandas>=2.0.0           # 데이터 처리
numpy>=1.24.0           # 수치 계산
ccxt>=4.0.0             # 거래소 API (계정 동기화)
python-dotenv>=1.0.0    # 환경 변수 관리
websocket-client>=1.5.0 # WebSocket 통신
requests>=2.31.0        # HTTP 요청
```

---

## 🎯 사용 방법

### 기본 실행 옵션

```bash
# 기본 실행
python main.py

# 계정 동기화 활성화 (실제 포지션 추적)
python main.py --account-sync

# 다른 심볼 거래
python main.py --symbol ETHUSDT

# 테스트넷 사용
python main.py --testnet

# 모든 옵션 조합
python main.py --symbol BTCUSDT --testnet --account-sync
```

### 주요 명령어 (클라우드)

```bash
# 서비스 상태 확인
systemctl status bitget-trading

# 실시간 로그 확인
tail -f /var/log/bitget/trading.log

# 서비스 재시작
systemctl restart bitget-trading

# 서비스 중지
systemctl stop bitget-trading

# 전체 시스템 상태
/usr/local/bin/bitget-status.sh
```

---

## 📊 로그 및 모니터링

### 로그 파일 위치

**로컬 실행:**
- `bitget_realtime_trading.log` - 메인 로그

**클라우드 배포:**
- `/var/log/bitget/trading.log` - 실시간 거래 로그
- `/var/log/bitget/error.log` - 에러 로그
- `/var/log/bitget/monitor.log` - 모니터링 로그
- `/var/log/bitget/install.log` - 설치 로그

### 로그 레벨

- **DEBUG**: 상세한 디버깅 정보
- **INFO**: 일반 정보 (거래 신호, 포지션 상태)
- **WARNING**: 경고 (연결 끊김, 동기화 실패)
- **ERROR**: 오류 (API 오류, 거래 실패)

---

## 🔐 보안 권장사항

### 로컬 개발

1. **환경 변수 사용**
   - `.env` 파일에 API 키 저장
   - 코드에 직접 하드코딩 금지

2. **Git 보안**
   - `.env` 파일을 `.gitignore`에 추가
   - API 키를 커밋하지 않도록 주의

3. **API 권한 제한**
   - 읽기 전용 권한 사용 (테스트 시)
   - 필요한 최소 권한만 부여

### 클라우드 배포

1. **SSH 키 인증 사용**
   - 비밀번호 인증 대신 SSH 키 사용
   - `VULTR_DEPLOYMENT_GUIDE.md` 참조

2. **방화벽 설정**
   - UFW로 불필요한 포트 차단
   - SSH 포트(22)만 열어둠

3. **IP 화이트리스트**
   - Bitget API에서 서버 IP만 허용

4. **민감 정보 삭제**
   ```bash
   # 배포 완료 후 토큰 삭제
   rm /root/.github_token
   ```

---

## 🧪 테스트

### API 연결 테스트

```bash
python check_api_keys.py
```

**출력 예시:**
```
✅ API 연결 성공
✅ 잔고 조회 성공: 1000.00 USDT
✅ 포지션 조회 성공
```

### 계정 통합 테스트

자세한 내용은 `README_ACCOUNT_INTEGRATION.md` 참조

---

## 📈 전략 파라미터

### 기본 설정 (`main.py`)

```python
strategy = SupportResistanceStrategy(
    account_manager=self.account_manager,
    investment_amount=1000.0,      # 초기 투자 금액 (USDT)
    profit=1.01,                   # 전체 익절 비율 (1%)
    partial_profit=1.008,          # 부분 익절 비율 (0.8%)
    
)
```

### 파라미터 설명

- **investment_amount**: 초기 투자 금액
- **profit**: 전체 익절 목표 수익률
- **partial_profit**: 부분 익절 목표 수익률


---

## 🐛 문제 해결

### 일반적인 문제

#### 1. API 연결 실패
```
❌ API 연결 실패
```
**해결:**
- `.env` 파일 확인
- API 키 유효성 확인
- 네트워크 연결 확인

#### 2. WebSocket 연결 끊김
```
[ERROR] WebSocket connection failed
```
**해결:**
- 자동 재연결 기능 내장
- 네트워크 안정성 확인
- 방화벽 설정 확인

#### 3. 서비스 시작 실패 (클라우드)
```
Active: failed (Result: exit-code)
```
**해결:**
```bash
# 로그 확인
journalctl -u bitget-trading -n 50

# API 키 재확인
nano /opt/bitget/.env

# 서비스 재시작
systemctl restart bitget-trading
```


---

## 💡 모범 사례

### 1. 테스트 먼저!
- 항상 테스트넷에서 먼저 테스트
- 소액으로 실전 테스트 후 증액

### 2. 리스크 관리
- 레버리지는 낮게 설정 (1-5x 권장)
- 손실 제한 (총 자산의 1-2%)
- 청산가 모니터링

### 3. 모니터링
- 정기적인 로그 확인
- 포지션 상태 실시간 추적
- 시스템 리소스 모니터링

### 4. 백업
- 정기적인 시스템 백업
- 로그 파일 보관
- 거래 내역 저장

---


## 📞 지원 및 문의

### 문제 발생 시

1. **로그 확인**: 에러 메시지 확인
2. **문서 참조**: README, 배포 가이드 확인
3. **공식 문서**: Bitget API, CCXT 문서 참조

### 유용한 링크

- **Bitget API 문서**: https://bitgetlimited.github.io/apidoc/
- **CCXT 문서**: https://docs.ccxt.com/
- **Vultr 문서**: https://www.vultr.com/docs/
- **Ubuntu 24.04 문서**: https://ubuntu.com/server/docs


---

## 🔄 버전 정보

- **현재 버전**: 1.0.0
- **마지막 업데이트**: 2024년 10월 8일
- **Python 버전**: 3.9+
- **지원 OS**: Windows, macOS, Linux (Ubuntu 24.04 권장)

---

## 🌟 주요 업데이트

### v1.0.0 (2024-10-08)
- ✅ Vultr 클라우드 자동 배포 지원
- ✅ Cloud-Init User-Data 스크립트 추가
- ✅ 자동 설정 스크립트 (`setup-cloud-init.sh`)
- ✅ 로그 자동 관리 (logrotate)
- ✅ 자동 모니터링 및 재시작
- ✅ 보안 강화 가이드

---

##  진행 중인 트레이딩 상황(실제 거래일 2025.12.09 ~ 현재)

### 트레이딩 뷰 백테스팅 기능을 활용하여 현재 전략의 상황을 표현
<img width="1804" height="582" alt="image" src="https://github.com/user-attachments/assets/95c75155-9189-4d24-ae20-a90e6b0283f8" />

### 트레이딩 지표 
- mdd : 44%, 누적 수익률 : 11.66%, 거래횟수 :40, 최대 사용 레버리지 : 5배, 한 거래 당 최대 손실 허용(전체 자본 기준) : 5%, 승률 44.7%, 사프지수 : 0.059, 소티노지수 : 0.11

### 진행 중인 전략에 대한 의견 : 
    - 레버리지 거래 특성 상 익절과 손절 모두 규모가 커질 수 있는데, 현재의 경우 손실 거래가 많아 mdd가 40%까지 증가했다. 최대 기간 백테스팅의 결과(2019.07 ~ 현재)의 mdd가 약 50% 이므로 아직 최대 위험에 도달하지는 않은 것으로 판단한다. 거래 기간 및 횟수가 많지 않아서 좀 더 진행해봐야 
       더 신뢰할 수 있을거 같다(대수의 법칙). 
    - 횡보장에 자주 거래가 발생하면 손절 횟수가 증가해 손실 금액이 커진다. 
    - 긍정적인 면은 거래 시작 후 연속 10번 정도 손절을 했지만 약 2번 정도의 익절을 통해 원금 회복 후 계좌는 이익 중이다. 손실 규모를 제한하고 익절 구간을 늘려서 손익비를 크게 가져가는 것이 장기적인 수익률에 매우 긍정적이라는 것을 다시 한 번 경험했다. 손익비가 크기 때문에 부분 익절을 통해          수익을 확보하면 손절이 발생 해도 전체 계좌 규모에서는 약간의 손해나 익절이 가능해서 장기적으로 계좌를 보호하는데 유리하다는 점도 깨달았다. 
    - 레버리지 거래가 위험하지만 감당할 수 있는 규모의 금액을 운용 중이기 때문에 장기적으로 유지해보려고 한다.

