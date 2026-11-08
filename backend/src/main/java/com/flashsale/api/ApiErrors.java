package com.flashsale.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
public class ApiErrors {
    private static final Logger log = LoggerFactory.getLogger(ApiErrors.class);

    @ExceptionHandler(ApiException.class)
    ResponseEntity<Contracts.ApiError> api(ApiException error) {
        return ResponseEntity.status(error.status()).body(new Contracts.ApiError(error.code(), error.getMessage()));
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, HttpMessageNotReadableException.class,
            MethodArgumentTypeMismatchException.class})
    ResponseEntity<Contracts.ApiError> invalid(Exception error) {
        return ResponseEntity.badRequest().body(new Contracts.ApiError("INVALID_INPUT", "请检查字段类型、必填项和取值范围"));
    }

    @ExceptionHandler(DataAccessException.class)
    ResponseEntity<Contracts.ApiError> database(DataAccessException error) {
        log.warn("Database operation failed: {}", error.getClass().getSimpleName());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new Contracts.ApiError("STORAGE_BUSY", "服务繁忙，请使用原请求编号重试"));
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<Contracts.ApiError> unexpected(Exception error) {
        log.error("Unexpected request failure", error);
        return ResponseEntity.internalServerError().body(new Contracts.ApiError("INTERNAL_ERROR", "服务处理失败，请查询原请求结果后重试"));
    }
}
