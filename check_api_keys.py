"""
비트겟 API 키 검증 스크립트

.env 파일의 API 키 설정을 확인하고 검증합니다.
"""

import os
import sys
from dotenv import load_dotenv

def check_env_file():
    """환경 변수 파일 확인"""
    print("[검증] .env 파일 확인 중...")
    
    # .env 파일 경로
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    
    if not os.path.exists(env_path):
        print("[오류] .env 파일이 없습니다!")
        print(f"파일 경로: {env_path}")
        print("\n[해결] 다음 내용으로 .env 파일을 생성하세요:")
        print("BITGET_API_KEY=your_api_key_here")
        print("BITGET_SECRET_KEY=your_secret_key_here")
        print("BITGET_PASSPHRASE=your_passphrase_here")
        return False
    
    print("[성공] .env 파일 발견")
    return True

def check_api_keys():
    """API 키 설정 확인"""
    print("\n[검증] API 키 설정 확인 중...")
    
    # .env 파일 로드
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    load_dotenv(env_path)
    
    # API 키 가져오기
    api_key = os.getenv('BITGET_API_KEY_SUB')
    secret_key = os.getenv('BITGET_SECRET_KEY_SUB')
    passphrase = os.getenv('BITGET_PASSPHRASE')
    
    print(f"API 키: {'설정됨' if api_key else 'None'}")
    print(f"시크릿 키: {'설정됨' if secret_key else 'None'}")
    print(f"패스프레이즈: {'설정됨' if passphrase else 'None'}")
    
    # 각 키 검증
    errors = []
    
    if not api_key:
        errors.append("API 키가 설정되지 않았습니다")
    elif len(api_key) < 20:
        errors.append("API 키가 너무 짧습니다 (최소 20자)")
    
    if not secret_key:
        errors.append("시크릿 키가 설정되지 않았습니다")
    elif len(secret_key) < 20:
        errors.append("시크릿 키가 너무 짧습니다 (최소 20자)")
    
    if not passphrase:
        errors.append("패스프레이즈가 설정되지 않았습니다")
    elif len(passphrase) < 5:
        errors.append("패스프레이즈가 너무 짧습니다 (최소 5자)")
    
    if errors:
        print("\n[오류] API 키 설정 문제:")
        for error in errors:
            print(f"  - {error}")
        return False
    
    print("\n[성공] API 키 설정이 올바릅니다!")
    return True

def check_api_key_format():
    """API 키 형식 검증"""
    print("\n[검증] API 키 형식 검증 중...")
    
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    load_dotenv(env_path)
    
    api_key = os.getenv('BITGET_API_KEY', '')
    secret_key = os.getenv('BITGET_SECRET_KEY', '')
    passphrase = os.getenv('BITGET_PASSPHRASE', '')
    
    # 마스킹된 키 표시
    masked_api_key = api_key[:8] + "..." + api_key[-4:] if len(api_key) > 12 else api_key
    masked_secret_key = secret_key[:8] + "..." + secret_key[-4:] if len(secret_key) > 12 else secret_key
    
    print(f"API 키: {masked_api_key} (길이: {len(api_key)})")
    print(f"시크릿 키: {masked_secret_key} (길이: {len(secret_key)})")
    print(f"패스프레이즈: {passphrase} (길이: {len(passphrase)})")
    
    # 형식 검증
    warnings = []
    
    if not api_key.startswith('bg_'):
        warnings.append("API 키가 'bg_'로 시작하지 않습니다")
    
    if len(api_key) < 30:
        warnings.append("API 키가 일반적인 길이보다 짧습니다")
    
    if len(secret_key) < 30:
        warnings.append("시크릿 키가 일반적인 길이보다 짧습니다")
    
    if warnings:
        print("\n[경고] API 키 형식 문제:")
        for warning in warnings:
            print(f"  - {warning}")
        print("\n[참고] 비트겟 API 키는 보통 다음과 같은 형식입니다:")
        print("  - API 키: bg_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        print("  - 시크릿 키: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        print("  - 패스프레이즈: 사용자가 설정한 비밀번호")
    else:
        print("\n[성공] API 키 형식이 올바릅니다!")
    
    return len(warnings) == 0

def main():
    """메인 함수"""
    print("[시작] 비트겟 API 키 검증")
    print("=" * 50)
    
    # 1. .env 파일 확인
    if not check_env_file():
        return False
    
    # 2. API 키 설정 확인
    if not check_api_keys():
        return False
    
    # 3. API 키 형식 검증
    format_ok = check_api_key_format()
    
    # 결과 요약
    print("\n" + "=" * 50)
    print("[결과] 검증 결과 요약")
    print("=" * 50)
    
    if format_ok:
        print("[완료] API 키 설정이 올바릅니다!")
        print("이제 test_connection.py를 실행해보세요.")
        return True
    else:
        print("[경고] API 키 형식에 문제가 있을 수 있습니다.")
        print("비트겟에서 새로 생성한 API 키인지 확인하세요.")
        return False

if __name__ == "__main__":
    try:
        success = main()
        exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n[중단] 사용자에 의해 검증이 중단되었습니다.")
        exit(1)
    except Exception as e:
        print(f"\n\n[오류] 예상치 못한 오류가 발생했습니다: {e}")
        exit(1)
