import {
  CallHandler,
  ExecutionContext,
  Inject,
  Injectable,
  NestInterceptor,
  SetMetadata,
  StreamableFile,
} from '@nestjs/common'
import { Observable, from, of, throwError } from 'rxjs'
import { catchError, mergeMap } from 'rxjs/operators'
import { assert } from './assert'
import { ZodDto, isZodDto } from './dto'
import { createZodSerializationException } from './exception'
import { UnknownSchema } from './types'

// NOTE (external)
// We need to deduplicate them here due to the circular dependency
// between core and common packages
const REFLECTOR = 'Reflector'

export const ZodSerializerDtoOptions = 'ZOD_SERIALIZER_DTO_OPTIONS' as const

export function ZodSerializerDto(
  dto:
    | ZodDto<UnknownSchema>
    | UnknownSchema
    | [ZodDto<UnknownSchema>]
    | [UnknownSchema]
) {
  if (Array.isArray(dto)) {
    const schema = 'schema' in dto[0] ? dto[0].schema : dto[0]
    assert(
      'array' in schema && typeof schema.array === 'function',
      'ZodSerializerDto was used with array syntax (e.g. `ZodSerializerDto([MyDto])`) but the DTO schema does not have an array method'
    )
  }

  return SetMetadata(ZodSerializerDtoOptions, dto)
}

@Injectable()
export class ZodSerializerInterceptor implements NestInterceptor {
  constructor(@Inject(REFLECTOR) protected readonly reflector: any) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const responseSchema = this.getContextResponseSchema(context)

    return next.handle().pipe(
      mergeMap((res: object | object[]) => {
        if (!responseSchema) return of(res)
        if (typeof res !== 'object' || res instanceof StreamableFile)
          return of(res)

        const resolveSchema = (
          schemaOrDto: UnknownSchema | ZodDto<UnknownSchema>
        ): UnknownSchema =>
          isZodDto(schemaOrDto) ? schemaOrDto.schema : schemaOrDto

        // Array syntax: @ZodSerializerDto([Dto])
        if (Array.isArray(responseSchema)) {
          const baseSchema = resolveSchema(responseSchema[0])
          assert(
            typeof baseSchema.array === 'function',
            'ZodSerializerDto was used with array syntax (e.g. `ZodSerializerDto([MyDto])`) but the DTO schema does not have an array method'
          )
          const arraySchema = baseSchema.array()
          const promise = arraySchema.parseAsync
            ? arraySchema.parseAsync(res)
            : Promise.resolve(arraySchema.parse(res))
          return from(promise).pipe(
            catchError((error) =>
              throwError(() => createZodSerializationException(error))
            )
          )
        }

        // Single schema
        const schema = resolveSchema(responseSchema)
        const promise = schema.parseAsync
          ? schema.parseAsync(res)
          : Promise.resolve(schema.parse(res))
        return from(promise).pipe(
          catchError((error) =>
            throwError(() => createZodSerializationException(error))
          )
        )
      })
    )
  }

  protected getContextResponseSchema(
    context: ExecutionContext
  ):
    | ZodDto<UnknownSchema>
    | UnknownSchema
    | [ZodDto<UnknownSchema>]
    | [UnknownSchema]
    | undefined {
    return this.reflector.getAllAndOverride(ZodSerializerDtoOptions, [
      context.getHandler(),
      context.getClass(),
    ])
  }
}
