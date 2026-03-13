# Coding Style Rules

## 1. Immutability
LUÔN tạo object mới, KHÔNG BAO GIỜ mutate. Dùng spread `{...obj}`.

## 2. File Size
200-400 dòng typical, 800 max.

## 3. Organize by Feature/Domain
Không organize by type (controllers/, models/).

## 4. High Cohesion, Low Coupling
Extract utilities từ large components.

## 5. Error Handling
LUÔN try/catch, throw Error với message user-friendly.

## 6. Input Validation
LUÔN validate bằng Zod schema.

## 7. Functions < 50 Lines
Nhỏ gọn, single responsibility.

## 8. No Deep Nesting
Max 4 levels. Early return thay vì nested if.

## 9. No console.log
Không có trong production code.

## 10. No Hardcoded Values
Dùng constants, env vars, config.
