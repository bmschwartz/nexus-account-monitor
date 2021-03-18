export async function asyncForEach(array: any[], callback: CallableFunction) {
  for (let index = 0; index < array.length; index++) {
    await callback(array[index], index, array);
  }
}

export function getAllSettledResults(results: PromiseSettledResult<any>[]) {
  return results.map((result: PromiseSettledResult<any>) => result.status === "fulfilled" ? result.value : null).filter(Boolean)
}
