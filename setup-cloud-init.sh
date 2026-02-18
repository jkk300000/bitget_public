#!/bin/bash
# Cloud-Init 설정 스크립트
# 이 스크립트는 cloud-init-userdata.yml 파일을 대화식으로 설정합니다.

set -e

echo "========================================="
echo "Bitget Cloud-Init 설정 도구"
echo "========================================="
echo ""

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 입력 파일 및 출력 파일 설정
INPUT_FILE="cloud-init-userdata.yml"
OUTPUT_FILE="cloud-init-userdata-configured.yml"

# 입력 파일 존재 확인
if [ ! -f "$INPUT_FILE" ]; then
    echo -e "${RED}❌ 오류: $INPUT_FILE 파일을 찾을 수 없습니다.${NC}"
    exit 1
fi

echo "이 스크립트는 Cloud-Init User-Data 파일을 자동으로 설정합니다."
echo ""
echo -e "${YELLOW}⚠️  주의: 민감한 정보(API 키, 토큰 등)를 입력하게 됩니다.${NC}"
echo -e "${YELLOW}   생성된 파일은 Git에 커밋하지 마세요!${NC}"
echo ""
read -p "계속하시겠습니까? (y/n): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "취소되었습니다."
    exit 1
fi

echo ""
echo "========================================="
echo "1. GitHub 설정"
echo "========================================="
echo ""

# GitHub Personal Access Token 입력
echo "GitHub Personal Access Token을 입력하세요:"
echo -e "${YELLOW}(형식: ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx)${NC}"
read -s -p "Token: " GITHUB_TOKEN
echo ""

if [ -z "$GITHUB_TOKEN" ]; then
    echo -e "${RED}❌ 오류: GitHub 토큰이 비어있습니다.${NC}"
    exit 1
fi

# GitHub 레포지토리 URL 입력
echo ""
echo "GitHub 레포지토리 URL을 입력하세요:"
echo -e "${YELLOW}(예: myusername/bitget-trading)${NC}"
read -p "Repository (username/repo): " GITHUB_REPO

if [ -z "$GITHUB_REPO" ]; then
    echo -e "${RED}❌ 오류: GitHub 레포지토리가 비어있습니다.${NC}"
    exit 1
fi

echo ""
echo "========================================="
echo "2. Bitget API 키 설정"
echo "========================================="
echo ""

# Bitget API 키 입력
read -s -p "Bitget API Key: " BITGET_API_KEY
echo ""
read -s -p "Bitget Secret Key: " BITGET_SECRET_KEY
echo ""
read -s -p "Bitget Passphrase: " BITGET_PASSPHRASE
echo ""

if [ -z "$BITGET_API_KEY" ] || [ -z "$BITGET_SECRET_KEY" ] || [ -z "$BITGET_PASSPHRASE" ]; then
    echo -e "${RED}❌ 오류: Bitget API 키 정보가 비어있습니다.${NC}"
    exit 1
fi

echo ""
echo "========================================="
echo "3. Discord 웹훅 설정 (선택사항)"
echo "========================================="
echo ""
echo "Discord 알림을 받으려면 웹훅 URL을 입력하세요."
echo -e "${YELLOW}(선택사항: Enter를 눌러 건너뛸 수 있습니다)${NC}"
echo ""
echo "Discord 웹훅 생성 방법:"
echo "  1. Discord 채널 > 채널 설정 > 연동 > 웹후크"
echo "  2. '새 웹후크 만들기' 클릭"
echo "  3. 웹후크 이름 설정 후 '웹후크 URL 복사' 클릭"
echo ""
read -p "Discord 웹훅 URL (선택사항): " DISCORD_WEBHOOK_URL

echo ""
echo "========================================="
echo "4. 파일 생성 중..."
echo "========================================="
echo ""

# 파일 복사 및 치환
cp "$INPUT_FILE" "$OUTPUT_FILE"

# GitHub 토큰 치환
sed -i.bak "s|ghp_your_github_token_here|$GITHUB_TOKEN|g" "$OUTPUT_FILE"

# GitHub 레포지토리 URL 치환
sed -i.bak "s|username/repository.git|$GITHUB_REPO.git|g" "$OUTPUT_FILE"

# Bitget API 키 치환
sed -i.bak "s|BITGET_API_KEY=your_api_key_here|BITGET_API_KEY=$BITGET_API_KEY|g" "$OUTPUT_FILE"
sed -i.bak "s|BITGET_SECRET_KEY=your_secret_key_here|BITGET_SECRET_KEY=$BITGET_SECRET_KEY|g" "$OUTPUT_FILE"
sed -i.bak "s|BITGET_PASSPHRASE=your_passphrase_here|BITGET_PASSPHRASE=$BITGET_PASSPHRASE|g" "$OUTPUT_FILE"

# Discord 웹훅 URL 치환
if [ -n "$DISCORD_WEBHOOK_URL" ]; then
    # 웹훅 URL이 입력된 경우
    sed -i.bak "s|https://discord.com/api/webhooks/YOUR_WEBHOOK_ID/YOUR_WEBHOOK_TOKEN|$DISCORD_WEBHOOK_URL|g" "$OUTPUT_FILE"
    echo -e "${GREEN}✅ Discord 웹훅 URL이 설정되었습니다.${NC}"
else
    # 웹훅 URL이 입력되지 않은 경우 - 기본값 유지 (주석 처리된 줄은 그대로 유지)
    echo -e "${YELLOW}⚠️  Discord 웹훅 URL이 설정되지 않았습니다. 배포 후 수동으로 설정할 수 있습니다.${NC}"
fi

# 백업 파일 삭제
rm -f "${OUTPUT_FILE}.bak"

echo -e "${GREEN}✅ Cloud-Init 파일이 생성되었습니다: $OUTPUT_FILE${NC}"
echo ""
echo "========================================="
echo "다음 단계:"
echo "========================================="
echo ""
echo "1. Vultr 콘솔에서 새 인스턴스 생성"
echo "2. Ubuntu 24.04 LTS x64 선택"
echo "3. 'Add Startup Script' 섹션에서 'User Data (Cloud-init)' 선택"
echo "4. $OUTPUT_FILE 파일의 내용을 복사하여 붙여넣기"
echo ""
echo "파일 내용 확인:"
echo "  cat $OUTPUT_FILE"
echo ""
echo "파일 복사 (클립보드):"
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "  cat $OUTPUT_FILE | pbcopy"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "  cat $OUTPUT_FILE | xclip -selection clipboard"
else
    echo "  파일 내용을 수동으로 복사하세요"
fi
echo ""
echo -e "${YELLOW}⚠️  보안 주의사항:${NC}"
echo "  - 이 파일에는 민감한 정보가 포함되어 있습니다"
echo "  - Git에 절대 커밋하지 마세요"
echo "  - 배포 완료 후 안전하게 삭제하세요: rm $OUTPUT_FILE"
echo ""
echo "========================================="

