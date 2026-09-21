import { createEslintConfig } from 'super-configs/eslint';

export default createEslintConfig({
  runtime: 'node',
  language: 'ts',
  typeChecked: true,
  testFramework: 'jest',
  ignores: ['dist/**', 'coverage/**', 'docs/api/**'],
});
