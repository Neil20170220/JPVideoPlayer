/*
 * This file is part of the JPVideoPlayer package.
 * (c) NewPan <13246884282@163.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * Click https://github.com/newyjp
 * or http://www.jianshu.com/users/e2f2d779c022/latest_articles to contact me.
 */

#import "JPVideoPlayerCacheFile.h"
#import "JPVideoPlayerCompat.h"
#import "JPVideoPlayerSupportUtils.h"
#import "JPVideoPlayerCompat.h"
#import <pthread.h>

///
/// methods start with `_internal` are private methods and must be called in the lock `lockQueue`
/// properties start with `internal` are private properties and must be accessed in the lock `lockQueue`
///

@interface JPVideoPlayerCacheFile ()

@property (nonatomic, strong) NSMutableArray<NSValue *> *internalFragmentRanges;

@property (nonatomic, strong) NSFileHandle *writeFileHandle;

@property (nonatomic, strong) NSFileHandle *readFileHandle;

@property (nonatomic, assign) BOOL completed;

@property (nonatomic, assign) NSUInteger internalFileLength;

@property (nonatomic, assign) NSUInteger internalReadOffset;

@property (nonatomic, copy) NSDictionary *responseHeaders;

@property (nonatomic, strong) dispatch_queue_t lockQueue;

@end

static const NSString *kJPVideoPlayerCacheFileZoneKey = @"com.newpan.zone.key.www";
static const NSString *kJPVideoPlayerCacheFileSizeKey = @"com.newpan.size.key.www";
static const NSString *kJPVideoPlayerCacheFileResponseHeadersKey = @"com.newpan.response.header.key.www";
@implementation JPVideoPlayerCacheFile

+ (instancetype)cacheFileWithFilePath:(NSString *)filePath
                        indexFilePath:(NSString *)indexFilePath {
    return [[self alloc] initWithFilePath:filePath
                            indexFilePath:indexFilePath];
}

- (instancetype)init {
    NSAssert(NO, @"Please use given initializer method");
    return [self initWithFilePath:@""
                    indexFilePath:@""];
}

- (instancetype)initWithFilePath:(NSString *)filePath
                   indexFilePath:(NSString *)indexFilePath {
    if (!filePath.length || !indexFilePath.length) {
        return nil;
    }
    
    self = [super init];
    if (self) {
        _cacheFilePath = filePath;
        _indexFilePath = indexFilePath;
        _internalFragmentRanges = [[NSMutableArray alloc] init];
        _readFileHandle = [NSFileHandle fileHandleForReadingAtPath:_cacheFilePath];
        _writeFileHandle = [NSFileHandle fileHandleForWritingAtPath:_cacheFilePath];
        _lockQueue = dispatch_queue_create("com.newpan.lockQueue", DISPATCH_QUEUE_SERIAL);
        
        NSString *indexStr = [NSString stringWithContentsOfFile:self.indexFilePath encoding:NSUTF8StringEncoding error:nil];
        NSData *data = [indexStr dataUsingEncoding:NSUTF8StringEncoding];
        NSDictionary *indexDictionary = nil;
        /// data = nil 导致闪退
        if (data) {
            indexDictionary = [NSJSONSerialization JSONObjectWithData:data
                                                              options:(NSJSONReadingMutableContainers | NSJSONReadingAllowFragments)
                                                                error:nil];
        }
        if (![self serializeIndex:indexDictionary]) {
            [self truncateFileWithFileLength:0];
        }
        
        [self _internalCheckIsCompleted];
    }
    return self;
}

- (void)dealloc {
    [self.readFileHandle closeFile];
    [self.writeFileHandle closeFile];
}


#pragma mark - Properties

- (NSUInteger)cachedDataBound {
    __block NSUInteger bound = 0;
    dispatch_sync(self.lockQueue, ^{
        NSArray *fragmentRanges = [self.internalFragmentRanges copy];
        if (fragmentRanges.count > 0) {
            NSRange range = [[fragmentRanges lastObject] rangeValue];
            bound = NSMaxRange(range);
        }
    });
    return bound;
}

- (BOOL)isFileLengthValid {
    return self.fileLength != 0;
}

- (BOOL)isCompleted {
    return self.completed;
}

- (BOOL)isEOF {
    __block BOOL eof = NO;
    dispatch_sync(self.lockQueue, ^{
        eof = self.internalReadOffset + 1 >= self.internalFileLength;
    });
    return eof;
}


#pragma mark - Range

- (NSArray<NSValue *> *)fragmentRanges {
    __block NSArray<NSValue *> *ranges;
    dispatch_sync(self.lockQueue, ^{
        ranges = [self.internalFragmentRanges copy];
    });
    return ranges;
}

- (NSUInteger)fileLength {
    __block NSUInteger length;
    dispatch_sync(self.lockQueue, ^{
        length = self.internalFileLength;
    });
    return length;
}

- (NSUInteger)readOffset {
    __block NSUInteger offset = 0;
    dispatch_sync(self.lockQueue, ^{
        offset = self.internalReadOffset;
    });
    return offset;
}

- (void)_internalMergeRangesIfNeed {
    BOOL isMerge = NO;
    for (int i = 0; i < self.internalFragmentRanges.count; ++i) {
        if ((i + 1) < self.internalFragmentRanges.count) {
            NSRange currentRange = [self.internalFragmentRanges[i] rangeValue];
            NSRange nextRange = [self.internalFragmentRanges[i + 1] rangeValue];
            if (JPRangeCanMerge(currentRange, nextRange)) {
                [self.internalFragmentRanges removeObjectsAtIndexes:[NSIndexSet indexSetWithIndexesInRange:NSMakeRange(i, 2)]];
                [self.internalFragmentRanges insertObject:[NSValue valueWithRange:NSUnionRange(currentRange, nextRange)] atIndex:i];
                i -= 1;
                isMerge = YES;
            }
        }
    }
    if (isMerge) {
        NSString *string = @"";
        for (NSValue *rangeValue in self.internalFragmentRanges) {
            NSRange range = [rangeValue rangeValue];
            string = [string stringByAppendingString:[NSString stringWithFormat:@"%@; ", NSStringFromRange(range)]];
        }
    }
}

- (void)addRange:(NSRange)range
      completion:(dispatch_block_t)completion {
    dispatch_sync(self.lockQueue, ^{
        if (range.length == 0 || range.location >= self.internalFileLength) {
            return;
        }
        
        BOOL inserted = NO;
        for (int i = 0; i < self.internalFragmentRanges.count; ++i) {
            NSRange currentRange = [self.internalFragmentRanges[i] rangeValue];
            if (currentRange.location >= range.location) {
                [self.internalFragmentRanges insertObject:[NSValue valueWithRange:range] atIndex:i];
                inserted = YES;
                break;
            }
        }
        if (!inserted) {
            [self.internalFragmentRanges addObject:[NSValue valueWithRange:range]];
        }
        [self _internalMergeRangesIfNeed];
        [self _internalCheckIsCompleted];
    });
    
    if (completion) {
        completion();
    }
}

- (NSRange)cachedRangeForRange:(NSRange)range {
    NSRange cachedRange = [self cachedRangeContainsPosition:range.location];
    NSRange ret = NSIntersectionRange(cachedRange, range);
    if (ret.length > 0) {
        return ret;
    } else {
        return JPInvalidRange;
    }
}

- (NSRange)cachedRangeContainsPosition:(NSUInteger)position {
    __block NSRange resultRange = JPInvalidRange;
    dispatch_sync(self.lockQueue, ^{
        resultRange = [self _internalCachedRangeContainsPosition:position];
    });
    return resultRange;
}

- (NSRange)_internalCachedRangeForRange:(NSRange)range {
    NSRange cachedRange = [self _internalCachedRangeContainsPosition:range.location];
    NSRange ret = NSIntersectionRange(cachedRange, range);
    if (ret.length > 0) {
        return ret;
    } else {
        return JPInvalidRange;
    }
}

- (NSRange)_internalCachedRangeContainsPosition:(NSUInteger)position {
    if (position >= self.internalFileLength) {
        return JPInvalidRange;
    }
    NSRange resultRange = JPInvalidRange;
    __block NSArray *fragmentRanges;
    fragmentRanges = [self.internalFragmentRanges copy];
    for (int i = 0; i < fragmentRanges.count; ++i) {
        NSRange range = [fragmentRanges[i] rangeValue];
        if (NSLocationInRange(position, range)) {
            resultRange = range;
            break;
        }
    }
    return resultRange;
}

- (NSRange)firstNotCachedRangeFromPosition:(NSUInteger)position {
    __block NSRange targetRange = JPInvalidRange;
    
    dispatch_sync(self.lockQueue, ^{
        if (position >= self.internalFileLength) {
            return;
        }
        NSUInteger start = position;
        NSArray *fragmentRanges = [self.internalFragmentRanges copy];
        for (int i = 0; i < fragmentRanges.count; ++i) {
            NSRange range = [fragmentRanges[i] rangeValue];
            if (NSLocationInRange(start, range)) {
                start = NSMaxRange(range);
            } else {
                if (start >= NSMaxRange(range)) {
                    continue;
                } else {
                    targetRange = NSMakeRange(start, range.location - start);
                }
            }
        }
        
        if (start < self.internalFileLength) {
            targetRange = NSMakeRange(start, self.internalFileLength - start);
        }
    });
    return targetRange;
}

- (void)_internalCheckIsCompleted {
    BOOL completed = NO;
    NSArray *fragmentRanges = [self.internalFragmentRanges copy];
    if (fragmentRanges && fragmentRanges.count == 1) {
        NSRange range = [fragmentRanges[0] rangeValue];
        if (range.location == 0 && (range.length == self.internalFileLength)) {
            completed = YES;
        }
    }
    self.completed = completed;
}


#pragma mark - File

- (BOOL)truncateFileWithFileLength:(NSUInteger)fileLength {
    JPDebugLog(@"Truncate file to length: %u", fileLength);
    if (!self.writeFileHandle) {
        return NO;
    }
    
    __block BOOL success = YES;
    dispatch_sync(self.lockQueue, ^{
        self.internalFileLength = fileLength;
        @try {
            [self.writeFileHandle truncateFileAtOffset:self.internalFileLength * sizeof(Byte)];
            unsigned long long end = [self.writeFileHandle seekToEndOfFile];
            if (end != self.internalFileLength) {
                success = NO;
            }
        }
        @catch (NSException *e) {
            JPErrorLog(@"Truncate file raise a exception: %@", e);
            success = NO;
        }
    });
    return success;
}

- (void)removeCache {
    [[NSFileManager defaultManager] removeItemAtPath:self.cacheFilePath error:NULL];
    [[NSFileManager defaultManager] removeItemAtPath:self.indexFilePath error:NULL];
}

- (BOOL)storeResponse:(NSHTTPURLResponse *)response {
    BOOL success = YES;
    if (![self isFileLengthValid]) {
        success = [self truncateFileWithFileLength:(NSUInteger)response.jp_fileLength];
    }
    self.responseHeaders = [[response allHeaderFields] copy];
    success = success && [self synchronize];
    return success;
}

- (void)storeVideoData:(NSData *)data
              atOffset:(NSUInteger)offset
           synchronize:(BOOL)synchronize
      storedCompletion:(dispatch_block_t)completion {
    @try {
        [self.writeFileHandle seekToFileOffset:offset];
        [self.writeFileHandle jp_safeWriteData:data];
    }
    @catch (NSException *e) {
        JPErrorLog(@"Write file raise a exception: %@", e);
    }
    
    [self addRange:NSMakeRange(offset, [data length])
        completion:completion];
    if (synchronize) {
        [self synchronize];
    }
}


#pragma mark - read data

- (NSData *)dataWithRange:(NSRange)range {
    if (!JPValidFileRange(range)) {
        return nil;
    }
    
    __block NSData *data = nil;
    dispatch_sync(self.lockQueue, ^{
        if (self.internalReadOffset != range.location) {
            [self _internalSeekToPosition:range.location];
        }
        data = [self _internalReadDataWithLength:range.length];
    });
    return data;
}

- (NSData *)readDataWithLength:(NSUInteger)length {
    __block NSData *result = nil;
    dispatch_sync(self.lockQueue, ^{
        result = [self _internalReadDataWithLength:length];
    });
    return result;
}

- (NSData *)_internalReadDataWithLength:(NSUInteger)length {
    NSRange range = [self _internalCachedRangeForRange:NSMakeRange(self.internalReadOffset, length)];
    if (JPValidFileRange(range)) {
        NSData *data = [self.readFileHandle readDataOfLength:range.length];
        self.internalReadOffset += [data length];
        return data;
    }
    return nil;
}


#pragma mark - seek

- (void)seekToPosition:(NSUInteger)position {
    dispatch_sync(self.lockQueue, ^{
        [self _internalSeekToPosition:position];
    });
}

- (void)_internalSeekToPosition:(NSUInteger)position {
    [self.readFileHandle seekToFileOffset:position];
    self.internalReadOffset = (NSUInteger)self.readFileHandle.offsetInFile;
}

- (void)seekToEnd {
    dispatch_sync(self.lockQueue, ^{
        [self.readFileHandle seekToEndOfFile];
        self.internalReadOffset = (NSUInteger)self.readFileHandle.offsetInFile;
    });
}


#pragma mark - Index

- (BOOL)serializeIndex:(NSDictionary *)indexDictionary {
    if (![indexDictionary isKindOfClass:[NSDictionary class]]) {
        return NO;
    }
    
    __block BOOL success = YES;
    dispatch_sync(self.lockQueue, ^{
        NSNumber *fileSize = indexDictionary[kJPVideoPlayerCacheFileSizeKey];
        if (fileSize && [fileSize isKindOfClass:[NSNumber class]]) {
            self.internalFileLength = [fileSize unsignedIntegerValue];
        }
        
        if (self.internalFileLength == 0) {
            success = NO;
            return;
        }
        
        [self.internalFragmentRanges removeAllObjects];
        NSMutableArray *rangeArray = indexDictionary[kJPVideoPlayerCacheFileZoneKey];
        for (NSString *rangeStr in rangeArray) {
            NSRange range = NSRangeFromString(rangeStr);
            [self.internalFragmentRanges addObject:[NSValue valueWithRange:range]];
        }
        self.responseHeaders = indexDictionary[kJPVideoPlayerCacheFileResponseHeadersKey];
    });
    return success;
}

- (NSString *)_internalUnserializeIndex {
    NSString *dataString = nil;
    NSMutableDictionary *dict = [@{
        kJPVideoPlayerCacheFileSizeKey: @(self.internalFileLength),
    } mutableCopy];
    
    NSMutableArray *rangeArray = [[NSMutableArray alloc] init];
    NSArray *fragmentRanges = [self.internalFragmentRanges copy];
    for (NSValue *range in fragmentRanges) {
        [rangeArray addObject:NSStringFromRange([range rangeValue])];
    }
    if (rangeArray.count) {
        dict[kJPVideoPlayerCacheFileZoneKey] = rangeArray;
    }
    
    JPDebugLog(@"存储字典: %@", dict);
    
    if (self.responseHeaders) {
        dict[kJPVideoPlayerCacheFileResponseHeadersKey] = self.responseHeaders;
    }
    
    NSData *data = [NSJSONSerialization dataWithJSONObject:dict options:0 error:nil];
    if (data) {
        dataString = [[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding];
    }
    return dataString;
}

- (BOOL)synchronize {
    __block BOOL synchronize = NO;
    dispatch_sync(self.lockQueue, ^{
        NSString *indexString = [self _internalUnserializeIndex];
        @try {
            [self.writeFileHandle synchronizeFile];
            synchronize = [indexString writeToFile:self.indexFilePath
                                        atomically:YES
                                          encoding:NSUTF8StringEncoding
                                             error:NULL];
            JPDebugLog(@"Did synchronize index file");
        } @catch (NSException *exception) {
            JPErrorLog(@"%@", exception);
        }
    });
    return synchronize;
}

@end
